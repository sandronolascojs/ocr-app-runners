import { inngest } from "@/inngest/client";
import { db } from "@/db";
import { ocrJobs, ocrJobFrames } from "@/db/schema";
import { eq } from "drizzle-orm";
import {
  extractTextFromCompletion,
  getBaseKeyFromFilename,
  type ChatCompletionContent,
} from "@/utils/ocr";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { InngestEvents } from "@/types/enums/inngest";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { getUserOpenAIClient } from "@/utils/openai-user";
import { BATCH_SIZE } from "./preprocessZip";

type ProcessedBatchResult = {
  batchId: string;
  batchOutputFileId: string;
  batchIndex: number;
};

type PersistableFrame = {
  jobId: string;
  filename: string;
  baseKey: string;
  index: number;
  text: string;
};

const saveBatchResults = async ({
  jobId,
  processedBatches,
  totalImages,
  openai,
}: {
  jobId: string;
  processedBatches: ProcessedBatchResult[];
  totalImages: number;
  openai: Awaited<ReturnType<typeof getUserOpenAIClient>>;
}) => {
  const framesToPersist: PersistableFrame[] = [];
  let totalProcessedLines = 0;

  // Process each batch output file
  for (const { batchOutputFileId, batchIndex } of processedBatches) {
    const outputStream = await openai.files.content(batchOutputFileId);
    const outputBuffer = Buffer.from(await outputStream.arrayBuffer());
    const outputJsonl = outputBuffer.toString("utf8");

    const lines = outputJsonl
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);

    if (!lines.length) {
      throw new Error(
        `Batch ${batchIndex} output file is empty for job ${jobId}.`
      );
    }

    for (const line of lines) {
      let parsed: {
        custom_id?: string;
        error?: { message?: string; code?: string };
        response?: {
          body?: {
            choices?: Array<{
              message?: { content?: ChatCompletionContent };
            }>;
          };
        };
      };

      try {
        parsed = JSON.parse(line) as {
          custom_id?: string;
          error?: { message?: string; code?: string };
          response?: {
            body?: {
              choices?: Array<{
                message?: { content?: ChatCompletionContent };
              }>;
            };
          };
        };
      } catch (error) {
        throw new Error(
          `Invalid JSON line in batch ${batchIndex} output: ${(error as Error).message}`
        );
      }

      if (parsed.error) {
        const message =
          parsed.error?.message ??
          parsed.error?.code ??
          "Unknown OpenAI batch error";
        throw new Error(
          `OpenAI batch ${batchIndex} entry failed (${parsed.custom_id ?? "unknown"}): ${message}`
        );
      }

      const customId = parsed.custom_id;
      if (!customId) {
        continue;
      }

      // Updated regex to match new custom_id format: job-{jobId}-batch-{batchIndex}-frame-{globalIndex}-{filename}
      const match = customId.match(/^job-(.+)-batch-(\d+)-frame-(\d+)-(.+)$/);
      if (!match) {
        continue;
      }

      const [, , , indexAsString, filename] = match;
      const index = Number.parseInt(indexAsString, 10);
      if (Number.isNaN(index)) {
        continue;
      }

      const completion =
        parsed.response?.body?.choices?.[0]?.message?.content;
      const text = extractTextFromCompletion(completion);

      if (!text || text === "<EMPTY>") {
        continue;
      }

      framesToPersist.push({
        jobId,
        filename,
        baseKey: getBaseKeyFromFilename(filename),
        index,
        text,
      });

      totalProcessedLines += 1;
    }
  }

  if (totalImages > 0 && totalProcessedLines !== totalImages) {
    throw new Error(
      `Batch output mismatch: expected ${totalImages} responses but got ${totalProcessedLines} across ${processedBatches.length} batches.`
    );
  }

  if (!framesToPersist.length) {
    throw new Error(
      `No OCR frames were parsed from the batch outputs (${processedBatches.length} batches).`
    );
  }

  await db.delete(ocrJobFrames).where(eq(ocrJobFrames.jobId, jobId));
  await db.insert(ocrJobFrames).values(framesToPersist);

  await db
    .update(ocrJobs)
    .set({ step: JobStep.RESULTS_SAVED })
    .where(eq(ocrJobs.jobId, jobId));
};

// This function processes all batch results when all batches are complete
// It collects batch info from the job and processes all results
export const processAllBatches = inngest.createFunction(
  {
    id: InngestFunctions.PROCESS_ALL_BATCHES,
    timeouts: {
      finish: "30m",
    },
  },
  { event: InngestEvents.ALL_BATCHES_COMPLETE },
  async ({ event, step }) => {
    const { jobId, userId } = event.data as {
      jobId: string;
      userId: string;
      totalBatches: number;
    };

    const openai = await getUserOpenAIClient(userId);

    // Get job to check current state
    const [job] = await db
      .select()
      .from(ocrJobs)
      .where(eq(ocrJobs.jobId, jobId))
      .limit(1);

    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }

    const totalImages = job.totalImages ?? 0;
    const expectedBatches = Math.ceil(totalImages / BATCH_SIZE);

    console.log(
      `Processing all ${expectedBatches} batches for job ${jobId}`
    );

    // Collect all batch info from error field (temporary storage)
    const processedBatches = await step.run("collect-batch-info", async () => {
      const [job] = await db
        .select()
        .from(ocrJobs)
        .where(eq(ocrJobs.jobId, jobId))
        .limit(1);

      if (!job) {
        throw new Error(`Job ${jobId} not found`);
      }

      // Parse batch info from error field
      let batchInfo: ProcessedBatchResult[] = [];

      if (job.error) {
        try {
          const parsed = JSON.parse(job.error) as Array<{
            batchId: string;
            batchOutputFileId: string;
            batchIndex: number;
          }>;
          batchInfo = parsed.map((b) => ({
            batchId: b.batchId,
            batchOutputFileId: b.batchOutputFileId,
            batchIndex: b.batchIndex,
          }));
        } catch (error) {
          throw new Error(
            `Failed to parse batch info from job ${jobId}: ${(error as Error).message}`
          );
        }
      }

      if (batchInfo.length !== expectedBatches) {
        throw new Error(
          `Expected ${expectedBatches} batches but found ${batchInfo.length} for job ${jobId}`
        );
      }

      // Sort by batchIndex to ensure correct order
      batchInfo.sort((a, b) => a.batchIndex - b.batchIndex);

      return batchInfo;
    });

    // Process all batch results
    await step.run("save-batch-results", async () => {
      await saveBatchResults({
        jobId,
        processedBatches,
        totalImages,
        openai,
      });
    });

    // Dispatch event to build documents
    await step.run("dispatch-build-documents", async () => {
      await inngest.send({
        name: InngestEvents.RESULTS_SAVED,
        data: {
          jobId,
          userId,
          totalImages,
        },
      });
    });

    return {
      jobId,
    };
  }
);

