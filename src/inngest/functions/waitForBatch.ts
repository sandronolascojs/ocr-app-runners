import { inngest } from "@/inngest/client";
import { db } from "@/db";
import { ocrJobs } from "@/db/schema";
import { eq } from "drizzle-orm";
import { getUserOpenAIClient } from "@/utils/openai-user";
import { InngestEvents } from "@/types/enums/inngest";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { OcrSleepId } from "@/types/enums/inngest";

const BATCH_SLEEP_INTERVAL = "20s";

const checkBatchStatus = async ({
  batchId,
  openai,
}: {
  batchId: string;
  openai: Awaited<ReturnType<typeof getUserOpenAIClient>>;
}): Promise<{
  status: string;
  outputFileId: string | null;
}> => {
  const latestBatch = await openai.batches.retrieve(batchId);

  return {
    status: latestBatch.status,
    outputFileId: latestBatch.output_file_id as string | null,
  };
};

export const waitForBatch = inngest.createFunction(
  {
    id: InngestFunctions.WAIT_FOR_BATCH,
    timeouts: {
      finish: "2h",
    },
  },
  { event: InngestEvents.BATCH_COMPLETE },
  async ({ event, step }) => {
    const { jobId, userId, batchId, batchIndex, totalBatches } = event.data as {
      jobId: string;
      userId: string;
      batchId: string;
      batchIndex: number;
      totalBatches: number;
    };

    const openai = await getUserOpenAIClient(userId);

    console.log(
      `Waiting for batch ${batchIndex + 1}/${totalBatches} (batchId: ${batchId}) to complete for job ${jobId}`
    );

    let batchOutputFileId: string | null = null;
    let attempt = 0;

    while (true) {
      const batchStatus = await step.run(
        `check-batch-status-${batchIndex}-${attempt}`,
        () =>
          checkBatchStatus({
            batchId,
            openai,
          })
      );

      if (
        batchStatus.status === "completed" &&
        batchStatus.outputFileId
      ) {
        batchOutputFileId = batchStatus.outputFileId;
        break;
      }

      if (
        batchStatus.status === "failed" ||
        batchStatus.status === "cancelled"
      ) {
        throw new Error(
          `Batch ${batchIndex} failed with status=${batchStatus.status}`
        );
      }

      // Sleep before next check - this must be outside step.run
      await step.sleep(
        `${OcrSleepId.WaitBatchCompletion}-${jobId}-${batchIndex}-${attempt}`,
        BATCH_SLEEP_INTERVAL
      );
      attempt += 1;
    }

    if (!batchOutputFileId) {
      throw new Error(
        `Batch ${batchIndex} completed but output file ID is missing`
      );
    }

    console.log(
      `Batch ${batchIndex + 1}/${totalBatches} completed for job ${jobId}`
    );

    // Store batch completion info in error field as JSON (temporary storage)
    // In production, you'd want a proper field for this
    await step.run(`store-batch-info-${batchIndex}`, async () => {
      const [job] = await db
        .select()
        .from(ocrJobs)
        .where(eq(ocrJobs.jobId, jobId))
        .limit(1);

      if (!job) {
        throw new Error(`Job ${jobId} not found`);
      }

      // Parse existing batch info from error field (used as temporary storage)
      let batchInfo: Array<{
        batchId: string;
        batchOutputFileId: string;
        batchIndex: number;
      }> = [];

      if (job.error) {
        try {
          batchInfo = JSON.parse(job.error) as typeof batchInfo;
        } catch {
          // If error field doesn't contain valid JSON, start fresh
          batchInfo = [];
        }
      }

      // Add this batch's info
      batchInfo.push({
        batchId,
        batchOutputFileId,
        batchIndex,
      });

      // Update job with batch info stored in error field (temporary)
      await db
        .update(ocrJobs)
        .set({
          batchId, // Keep last batch ID for backward compatibility
          batchOutputFileId, // Keep last batch output file ID for backward compatibility
          error: JSON.stringify(batchInfo), // Store all batch info in error field temporarily
        })
        .where(eq(ocrJobs.jobId, jobId));

      // If this is the last batch, trigger processing of all batches
      if (batchIndex === totalBatches - 1) {
        await inngest.send({
          name: InngestEvents.ALL_BATCHES_COMPLETE,
          data: {
            jobId,
            userId,
            totalBatches,
          },
        });
      }
    });

    return {
      jobId,
      batchId,
      batchIndex,
      batchOutputFileId,
    };
  }
);

