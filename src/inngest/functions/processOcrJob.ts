import * as fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import archiver from "archiver";
import { Transform } from "node:stream";
import unzipper from "unzipper";
import { inngest } from "@/inngest/client";
import { db } from "@/db";
import { ocrJobs, ocrJobFrames } from "@/db/schema";
import { eq } from "drizzle-orm";
import {
  validateProcessableImageEntry,
  getBaseKeyFromFilename,
  compareImageFilenames,
  extractTextFromCompletion,
  normalizeBufferTo1280x720,
  cropSubtitleFromBuffer,
  createThumbnailFromBuffer,
  type ChatCompletionContent,
} from "@/utils/ocr";
import {
  getJobRootDir,
  getJobRawDir,
  getJobNormalizedDir,
  getJobCropsDir,
  getJobTxtPath,
  getJobDocxPath,
  getJobBatchJsonlPath,
  getJobRawArchivePath,
  getJobZipPath,
  VOLUME_DIRS,
} from "@/utils/paths";
import { writeDocxFromParagraphs } from "@/utils/ocr/docx";
import { buildParagraphsFromFrames } from "@/utils/ocr/paragraphs";
import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { InngestEvents, OcrStepId, OcrSleepId } from "@/types/enums/inngest";
import { getUserOpenAIClient } from "@/utils/openai-user";
import type { OpenAI } from "openai";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { AI_CONSTANTS } from "@/constants/ai.constants";
import {
  getJobDocxKey,
  getJobRawArchiveKey,
  getJobTxtKey,
  getJobCropKey,
  getJobThumbnailKey,
  uploadFileToObject,
  uploadBufferToObject,
  uploadStreamToObject,
  createSignedDownloadUrlWithTtl,
  downloadObjectStream,
} from "@/utils/storage";

const BATCH_SLEEP_INTERVAL = "20s";
const BATCH_SIZE = 20;

type CropMeta = {
  filename: string;
  cropKey: string;
  cropSignedUrl: string;
};

type WorkspacePaths = {
  jobRootDir: string;
  rawDir: string;
  normalizedDir: string;
  cropsDir: string;
  txtPath: string;
  docxPath: string;
  batchJsonlPath: string; // Legacy path, now using getJobBatchJsonlPath with batchIndex
  zipPath: string;
  rawArchivePath: string;
};

type StorageKeys = {
  txtKey: string;
  docxKey: string;
  rawZipKey: string;
};

type StreamingArtifacts = {
  totalImages: number;
  rawZipKey: string | null;
  rawZipSizeBytes: number | null;
  thumbnailKey: string | null;
  cropsMetaPath: string;
};

type BatchArtifacts = {
  batchId: string;
  batchInputFileId: string;
  batchIndex: number;
};

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

const CROP_SIGNED_URL_TTL_SECONDS = 60 * 60 * 24; // 24 hours

const streamAndProcessZip = async ({
  jobId,
  zipKey,
  storageKeys,
  cropsMetaPath,
}: {
  jobId: string;
  zipKey: string;
  storageKeys: StorageKeys;
  cropsMetaPath: string;
}): Promise<StreamingArtifacts> => {
  const zipReadable = await downloadObjectStream(zipKey);
  const unzipStream = zipReadable.pipe(unzipper.Parse({ forceStream: true }));

  const archive = archiver("zip", { zlib: { level: 9 } });
  let filteredZipSizeBytes = 0;
  const sizeCounter = new Transform({
    transform(chunk, _encoding, callback) {
      filteredZipSizeBytes += chunk.length;
      callback(null, chunk);
    },
  });

  const archiveOutput = archive.pipe(sizeCounter);
  const filteredZipUploadPromise = uploadStreamToObject({
    key: storageKeys.rawZipKey,
    stream: archiveOutput,
    contentType: "application/zip",
  });

  const cropsMeta: CropMeta[] = [];
  let processedImages = 0;
  let thumbnailKey: string | null = null;

  for await (const entry of unzipStream) {
    if (entry.type === "Directory") {
      entry.autodrain();
      continue;
    }

    const entryName = entry.path;
    const processable = validateProcessableImageEntry(entryName);
    if (!processable) {
      entry.autodrain();
      continue;
    }

    const fileBuffer = await entry.buffer();
    const normalizedBuffer = await normalizeBufferTo1280x720(fileBuffer);
    const cropBuffer = await cropSubtitleFromBuffer(normalizedBuffer);

    // Only include base images (1, 2, 3, etc.) in the final ZIP
    // Skip decimal variants (1.1, 1.2, etc.) from the ZIP
    // Use original image (raw) with original extension
    if (processable.shouldIncludeInZip) {
      // Extract original extension from originalName (e.g., "1.jpg" -> ".jpg")
      const originalExt = processable.originalName.match(/\.(png|jpe?g)$/i)?.[0] || '.png';
      const zipFilename = `${processable.baseName}${originalExt}`;
      archive.append(fileBuffer, { name: zipFilename });
    }

    // Create crop for ALL images (including 1.1, 1.2, etc.) for OCR processing
    // Use original filename to preserve the relationship
    const cropFilename = processable.originalName.replace(
      /\.(png|jpe?g)$/i,
      ".png"
    );
    const cropKey = getJobCropKey(jobId, cropFilename);
    await uploadBufferToObject({
      key: cropKey,
      body: cropBuffer,
      contentType: "image/png",
    });

    const signedCropUrl = await createSignedDownloadUrlWithTtl({
      key: cropKey,
      responseContentType: "image/png",
      downloadFilename: cropFilename,
      ttlSeconds: CROP_SIGNED_URL_TTL_SECONDS,
    });

    cropsMeta.push({
      filename: cropFilename,
      cropKey,
      cropSignedUrl: signedCropUrl.url,
    });

    if (!thumbnailKey) {
      const thumbnailBuffer = await createThumbnailFromBuffer(normalizedBuffer);
      const thumbKey = getJobThumbnailKey(jobId);
      await uploadBufferToObject({
        key: thumbKey,
        body: thumbnailBuffer,
        contentType: "image/jpeg",
        cacheControl: "public, max-age=31536000, immutable",
      });
      thumbnailKey = thumbKey;
    }

    processedImages += 1;
    await db
      .update(ocrJobs)
      .set({
        processedImages,
        totalImages: processedImages,
        status: JobsStatus.PROCESSING,
      })
      .where(eq(ocrJobs.jobId, jobId));
  }

  await archive.finalize();
  await filteredZipUploadPromise;

  const sortedCrops = [...cropsMeta].sort((a, b) => {
    const comparison = compareImageFilenames(a.filename, b.filename);
    if (comparison !== 0) {
      return comparison;
    }
    return a.filename.localeCompare(b.filename);
  });

  await fs.writeFile(cropsMetaPath, JSON.stringify(sortedCrops), "utf8");

  return {
    totalImages: processedImages,
    rawZipKey: processedImages > 0 ? storageKeys.rawZipKey : null,
    rawZipSizeBytes: processedImages > 0 ? filteredZipSizeBytes : null,
    thumbnailKey,
    cropsMetaPath,
  };
};

/**
 * Divides an array into chunks of specified size
 */
const chunkArray = <T>(array: T[], chunkSize: number): T[][] => {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
};

const createBatchArtifacts = async ({
  jobId,
  cropsMeta,
  batchIndex,
  globalStartIndex,
  openai,
}: {
  jobId: string;
  cropsMeta: CropMeta[];
  batchIndex: number;
  globalStartIndex: number;
  openai: OpenAI;
}): Promise<BatchArtifacts> => {
  if (!cropsMeta.length) {
    throw new Error(
      `No crops found for job ${jobId} batch ${batchIndex} when creating Batch artifacts.`
    );
  }

  const batchJsonlPath = getJobBatchJsonlPath(jobId, batchIndex);
  const jsonlStream = fsSync.createWriteStream(batchJsonlPath, {
    encoding: "utf8",
  });

  // Register error handler immediately to catch errors from write() calls
  const streamPromise = new Promise<void>((resolve, reject) => {
    jsonlStream.on("error", (err) => reject(err));
    jsonlStream.on("finish", () => resolve());
  });

  // Write all lines for this batch chunk
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
    batchIndex,
  };
};

const checkBatchStatus = async ({
  batchId,
  openai,
}: {
  batchId: string;
  openai: OpenAI;
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

const saveBatchResults = async ({
  jobId,
  processedBatches,
  totalImages,
  openai,
}: {
  jobId: string;
  processedBatches: ProcessedBatchResult[];
  totalImages: number;
  openai: OpenAI;
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
    .set({ step: JobStep.DOCS_BUILT })
    .where(eq(ocrJobs.jobId, jobId));
};

const buildDocuments = async ({
  jobId,
  paths,
  storageKeys,
  totalImages,
}: {
  jobId: string;
  paths: WorkspacePaths;
  storageKeys: StorageKeys;
  totalImages: number;
}): Promise<string | null> => {
  const frames = await db
    .select()
    .from(ocrJobFrames)
    .where(eq(ocrJobFrames.jobId, jobId));

  const paragraphs = buildParagraphsFromFrames(frames);
  if (!paragraphs.length) {
    throw new Error("Unable to build OCR paragraphs for this job.");
  }

  const paragraphsWithBlankLine = paragraphs.flatMap((paragraph, index) =>
    index < paragraphs.length - 1 ? [paragraph, ""] : [paragraph]
  );

  const txtContent = paragraphsWithBlankLine.join("\n");

  await fs.writeFile(paths.txtPath, txtContent, "utf8");
  await writeDocxFromParagraphs(paragraphs, paths.docxPath);

  // Calculate file sizes before uploading
  const txtStats = fsSync.statSync(paths.txtPath);
  const docxStats = fsSync.statSync(paths.docxPath);

  await uploadFileToObject({
    key: storageKeys.txtKey,
    filePath: paths.txtPath,
    contentType: "text/plain; charset=utf-8",
  });

  await uploadFileToObject({
    key: storageKeys.docxKey,
    filePath: paths.docxPath,
    contentType:
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  });

  // Update job with documents info
  await db
    .update(ocrJobs)
    .set({
      status: JobsStatus.DONE,
      txtPath: storageKeys.txtKey,
      docxPath: storageKeys.docxKey,
      txtSizeBytes: txtStats.size,
      docxSizeBytes: docxStats.size,
    })
    .where(eq(ocrJobs.jobId, jobId));

  const dirsToRemove = [paths.rawDir, paths.normalizedDir, paths.cropsDir];
  const filesToRemove = [
    paths.zipPath,
    paths.txtPath,
    paths.docxPath,
    paths.rawArchivePath,
    path.join(paths.cropsDir, "cropsMeta.json"),
  ];

  // Remove all batch JSONL files (legacy and new batch-indexed files)
  // Try to remove the legacy file first
  try {
    await fs.unlink(paths.batchJsonlPath);
  } catch {
    // ignore
  }

  // Remove all batch-indexed files
  // Calculate the number of batches based on totalImages
  const numberOfBatches = Math.ceil(totalImages / BATCH_SIZE);
  // Add a buffer to ensure we clean up all files even if there's a slight mismatch
  const maxBatchesToCheck = numberOfBatches + 10;
  
  for (let i = 0; i < maxBatchesToCheck; i++) {
    try {
      const batchPath = getJobBatchJsonlPath(jobId, i);
      await fs.unlink(batchPath);
    } catch {
      // File doesn't exist or already removed, continue to next
      // Don't break here as batches might not be sequential if there were errors
    }
  }

  for (const file of filesToRemove) {
    try {
      await fs.unlink(file);
    } catch {
      // ignore
    }
  }

  for (const dir of dirsToRemove) {
    try {
      await fs.rm(dir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }

  // Return raw zip key (already saved in DB earlier, use storage key)
  return storageKeys.rawZipKey;
};

const buildWorkspacePaths = (jobId: string): WorkspacePaths => ({
  jobRootDir: getJobRootDir(jobId),
  rawDir: getJobRawDir(jobId),
  normalizedDir: getJobNormalizedDir(jobId),
  cropsDir: getJobCropsDir(jobId),
  txtPath: getJobTxtPath(jobId),
  docxPath: getJobDocxPath(jobId),
  batchJsonlPath: getJobBatchJsonlPath(jobId),
  zipPath: getJobZipPath(jobId),
  rawArchivePath: getJobRawArchivePath(jobId),
});

const buildStorageKeys = (jobId: string): StorageKeys => ({
  txtKey: getJobTxtKey(jobId),
  docxKey: getJobDocxKey(jobId),
  rawZipKey: getJobRawArchiveKey(jobId),
});

const ensureWorkspaceLayout = async (paths: WorkspacePaths) => {
  await fs.mkdir(paths.jobRootDir, { recursive: true });
  await fs.mkdir(paths.rawDir, { recursive: true });
  await fs.mkdir(paths.normalizedDir, { recursive: true });
  await fs.mkdir(paths.cropsDir, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.txtBase, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.wordBase, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.tmpBase, { recursive: true });
};

export const processOcrJob = inngest.createFunction(
  {
    id: InngestFunctions.PROCESS_OCR_JOB,
    timeouts: {
      finish: "2h", // Maximum allowed timeout for processing large batches of images
    },
  },
  { event: InngestEvents.ZIP_UPLOADED },
  async ({ event, step }): Promise<{
    jobId: string;
    txtKey: string;
    docxKey: string;
    rawZipKey: string | null;
  }> => {
    const { jobId, zipKey, userId } = event.data as {
      jobId: string;
      zipKey: string;
      userId: string;
    };

    if (!userId) {
      console.error("UserId missing in event data", event.data);
      
      try {
        await db
          .update(ocrJobs)
          .set({
            status: JobsStatus.ERROR,
            error: "UserId missing in event data",
          })
          .where(eq(ocrJobs.jobId, jobId));
      } catch (updateError) {
        console.error(
          `Failed to update job ${jobId} to ERROR state:`,
          updateError
        );
      }
      
      return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
    }

    try {
      // Get user's OpenAI client
      const openai = await getUserOpenAIClient(userId);

      const [job] = await db
        .select()
        .from(ocrJobs)
        .where(eq(ocrJobs.jobId, jobId))
        .limit(1);

      if (!job) {
        console.error("Job not found", jobId);
        return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
      }

      const storageZipKey = zipKey ?? job.zipPath;
      if (!storageZipKey) {
        console.error("Zip key missing for job", jobId);
        return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
      }

      // Estado actual en memoria (se irá actualizando manualmente)
      let currentStep: JobStep = job.step ?? JobStep.PREPROCESSING;
      let totalImages = job.totalImages ?? 0;

      const workspacePaths = buildWorkspacePaths(jobId);
      const storageKeys = buildStorageKeys(jobId);
  const cropsMetaPath = path.join(workspacePaths.cropsDir, "cropsMeta.json");
      let rawZipKeyForJob: string | null = job.rawZipPath ?? null;

      await ensureWorkspaceLayout(workspacePaths);

      const streamingResult = await step.run(
        OcrStepId.PreprocessImagesAndCrops,
        () =>
          streamAndProcessZip({
            jobId,
            zipKey: storageZipKey,
            storageKeys,
        cropsMetaPath,
          })
      );

      totalImages = streamingResult.totalImages;
      rawZipKeyForJob = streamingResult.rawZipKey;

      await db
        .update(ocrJobs)
        .set({
          rawZipPath: streamingResult.rawZipKey,
          rawZipSizeBytes: streamingResult.rawZipSizeBytes,
          thumbnailKey: streamingResult.thumbnailKey,
          step: JobStep.BATCH_SUBMITTED,
          totalImages,
          processedImages: totalImages,
          status: JobsStatus.PROCESSING,
        })
        .where(eq(ocrJobs.jobId, jobId));

  const cropsMeta: CropMeta[] = JSON.parse(
    fsSync.readFileSync(cropsMetaPath, "utf8")
  ) as CropMeta[];
      if (!cropsMeta.length) {
        throw new Error("No crops were generated from the provided ZIP file.");
      }
      currentStep = JobStep.BATCH_SUBMITTED;

      if (currentStep === JobStep.BATCH_SUBMITTED) {
        // Divide cropsMeta into chunks of BATCH_SIZE (no limit on number of batches)
        const cropsChunks = chunkArray(cropsMeta, BATCH_SIZE);
        const processedBatches: ProcessedBatchResult[] = [];
        const totalBatches = cropsChunks.length;

        console.log(
          `Processing ${totalImages} images in ${totalBatches} batches of ${BATCH_SIZE} images each for job ${jobId}`
        );

        // Process each batch chunk sequentially - wait for each batch to complete before starting the next
        for (let batchIndex = 0; batchIndex < cropsChunks.length; batchIndex++) {
          const chunk = cropsChunks[batchIndex];
          const globalStartIndex = batchIndex * BATCH_SIZE;

          console.log(
            `Creating batch ${batchIndex + 1}/${totalBatches} for job ${jobId} (images ${globalStartIndex + 1}-${globalStartIndex + chunk.length})`
          );

          // Create batch artifacts (JSONL file and OpenAI batch)
          const artifacts = await step.run(
            `${OcrStepId.CreateAndAwaitBatch}-${batchIndex}`,
            () =>
              createBatchArtifacts({
                jobId,
                cropsMeta: chunk,
                batchIndex,
                globalStartIndex,
                openai,
              })
          );

          if (!artifacts.batchId) {
            throw new Error(
              `Batch ID missing after creation for batch ${batchIndex}.`
            );
          }

          console.log(
            `Waiting for batch ${batchIndex + 1}/${totalBatches} (batchId: ${artifacts.batchId}) to complete for job ${jobId}`
          );

          // Wait for this batch to complete before processing the next one
          // This ensures we don't exceed OpenAI rate limits and process sequentially
          // Use separate steps for each check to allow proper sleep handling
          let batchOutputFileId: string | null = null;
          let attempt = 0;
          
          while (true) {
            const batchStatus = await step.run(
              `${OcrStepId.WaitBatchCompletion}-${batchIndex}-${attempt}`,
              () =>
                checkBatchStatus({
                  batchId: artifacts.batchId,
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

          processedBatches.push({
            batchId: artifacts.batchId,
            batchOutputFileId,
            batchIndex,
          });

          // Update job with the last batch info (for tracking purposes)
          await db
            .update(ocrJobs)
            .set({
              batchId: artifacts.batchId,
              batchInputFileId: artifacts.batchInputFileId,
              batchOutputFileId,
            })
            .where(eq(ocrJobs.jobId, jobId));
        }

        console.log(
          `All ${totalBatches} batches completed for job ${jobId}. Processing results...`
        );

        await db
          .update(ocrJobs)
          .set({
            step: JobStep.RESULTS_SAVED,
          })
          .where(eq(ocrJobs.jobId, jobId));

        currentStep = JobStep.RESULTS_SAVED;

        // Save all batch results
        await step.run(OcrStepId.SaveResultsToDb, () =>
          saveBatchResults({
            jobId,
            processedBatches,
            totalImages,
            openai,
          })
        );

        currentStep = JobStep.DOCS_BUILT;
      }

      if (currentStep === JobStep.DOCS_BUILT) {
        rawZipKeyForJob = await step.run(OcrStepId.BuildDocsAndCleanup, () =>
          buildDocuments({
            jobId,
            paths: workspacePaths,
            storageKeys,
            totalImages,
          })
        );

        // Ensure the job reflects the final step in case any prior update was skipped
        await db
          .update(ocrJobs)
          .set({
            step: JobStep.DOCS_BUILT,
            status: JobsStatus.DONE,
          })
          .where(eq(ocrJobs.jobId, jobId));
      }

      return {
        jobId,
        txtKey: storageKeys.txtKey,
        docxKey: storageKeys.docxKey,
        rawZipKey: rawZipKeyForJob,
      };
    } catch (err) {
      console.error("processOcrJob failed", jobId, err);

      const errorMessage =
        err instanceof Error ? err.message : "Unknown error in OCR job";

      // Guardar error y marcar job como ERROR; el retry lo relanza desde el step que quedó
      await db
        .update(ocrJobs)
        .set({
          status: JobsStatus.ERROR,
          error: errorMessage,
        })
        .where(eq(ocrJobs.jobId, jobId));

      throw err;
    }
  }
);