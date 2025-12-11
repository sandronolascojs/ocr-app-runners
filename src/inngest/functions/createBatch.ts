import fsSync from "node:fs";
import { inngest } from "@/inngest/client";
import { getUserOpenAIClient } from "@/utils/openai-user";
import { getJobBatchJsonlPath } from "@/utils/paths";
import { InngestEvents } from "@/types/enums/inngest";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { AI_CONSTANTS } from "@/constants/ai.constants";

type CropMeta = {
  filename: string;
  cropKey: string;
  cropSignedUrl: string;
};

export const createBatch = inngest.createFunction(
  {
    id: InngestFunctions.CREATE_BATCH,
    timeouts: {
      finish: "10m",
    },
  },
  { event: InngestEvents.BATCH_CREATE },
  async ({ event, step }) => {
    const {
      jobId,
      userId,
      batchIndex,
      totalBatches,
      cropsMeta,
      globalStartIndex,
    } = event.data as {
      jobId: string;
      userId: string;
      batchIndex: number;
      totalBatches: number;
      cropsMeta: CropMeta[];
      globalStartIndex: number;
    };

    if (!cropsMeta.length) {
      throw new Error(
        `No crops found for job ${jobId} batch ${batchIndex} when creating Batch artifacts.`
      );
    }

    const openai = await getUserOpenAIClient(userId);

    const batchArtifacts = await step.run(
      `create-batch-${batchIndex}`,
      async () => {
        const batchJsonlPath = getJobBatchJsonlPath(jobId, batchIndex);
        const jsonlStream = fsSync.createWriteStream(batchJsonlPath, {
          encoding: "utf8",
        });

        const streamPromise = new Promise<void>((resolve, reject) => {
          jsonlStream.on("error", (err) => reject(err));
          jsonlStream.on("finish", () => resolve());
        });

        for (let localIndex = 0; localIndex < cropsMeta.length; localIndex++) {
          const { filename, cropSignedUrl } = cropsMeta[localIndex];
          const globalIndex = globalStartIndex + localIndex;
          const customId = `job-${jobId}-batch-${batchIndex}-frame-${globalIndex}-${filename}`;

          const line = {
            custom_id: customId,
            method: "POST",
            url: "/v1/chat/completions",
            body: {
              model: AI_CONSTANTS.MODELS.OPENAI,
              temperature: 0,
              max_tokens: 96,
              messages: [
                {
                  role: "user",
                  content: [
                    { type: "text", text: AI_CONSTANTS.PROMPTS.OCR },
                    {
                      type: "image_url",
                      image_url: { url: cropSignedUrl },
                    },
                  ],
                },
              ],
            },
          };

          jsonlStream.write(JSON.stringify(line) + "\n");
        }

        jsonlStream.end();
        await streamPromise;

        const inputFile = await openai.files.create({
          file: fsSync.createReadStream(batchJsonlPath),
          purpose: "batch",
        });

        const batch = await openai.batches.create({
          input_file_id: inputFile.id,
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
        });

        return {
          batchId: batch.id,
          batchInputFileId: inputFile.id,
        };
      }
    );

    console.log(
      `Batch ${batchIndex + 1}/${totalBatches} created (batchId: ${batchArtifacts.batchId}) for job ${jobId}`
    );

    // Dispatch event to wait for this batch completion
    await step.run(`dispatch-wait-batch-${batchIndex}`, async () => {
      await inngest.send({
        name: InngestEvents.BATCH_COMPLETE,
        data: {
          jobId,
          userId,
          batchId: batchArtifacts.batchId,
          batchIndex,
          totalBatches,
        },
      });
    });

    return {
      jobId,
      batchId: batchArtifacts.batchId,
      batchIndex,
    };
  }
);

