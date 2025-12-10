import * as fs from "node:fs/promises";
import fsSync from "node:fs";
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
  batchJsonlPath: string;
  zipPath: string;
  rawArchivePath: string;
};

type StorageKeys = {
  txtKey: string;
  docxKey: string;
  rawZipKey: string;
};

type StreamingArtifacts = {
  cropsMeta: CropMeta[];
  totalImages: number;
  rawZipKey: string | null;
  rawZipSizeBytes: number | null;
  thumbnailKey: string | null;
};

type BatchArtifacts = {
  batchId: string;
  batchInputFileId: string;
};

type PersistableFrame = {
  jobId: string;
  filename: string;
  baseKey: string;
  index: number;
  text: string;
};

type SleepFn = (id: string, duration: string) => Promise<void>;

const CROP_SIGNED_URL_TTL_SECONDS = 60 * 60 * 24; // 24 hours

const streamAndProcessZip = async ({
  jobId,
  zipKey,
  storageKeys,
}: {
  jobId: string;
  zipKey: string;
  storageKeys: StorageKeys;
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

  return {
    cropsMeta: sortedCrops,
    totalImages: processedImages,
    rawZipKey: processedImages > 0 ? storageKeys.rawZipKey : null,
    rawZipSizeBytes: processedImages > 0 ? filteredZipSizeBytes : null,
    thumbnailKey,
  };
};

const createBatchArtifacts = async ({
  jobId,
  cropsMeta,
  paths,
  openai,
}: {
  jobId: string;
  cropsMeta: CropMeta[];
  paths: WorkspacePaths;
  openai: OpenAI;
}): Promise<BatchArtifacts> => {
  if (!cropsMeta.length) {
    throw new Error(
      `No crops found for job ${jobId} when creating Batch artifacts.`
    );
  }

  const jsonlStream = fsSync.createWriteStream(paths.batchJsonlPath, {
    encoding: "utf8",
  });

  // Register error handler immediately to catch errors from write() calls
  const streamPromise = new Promise<void>((resolve, reject) => {
    jsonlStream.on("error", (err) => reject(err));
    jsonlStream.on("finish", () => resolve());
  });

  // Write all lines
  for (let index = 0; index < cropsMeta.length; index++) {
    const { filename, cropSignedUrl } = cropsMeta[index];
    const customId = `job-${jobId}-frame-${index}-${filename}`;

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
    file: fsSync.createReadStream(paths.batchJsonlPath),
    purpose: "batch",
  });

  const batch = await openai.batches.create({
    input_file_id: inputFile.id,
    endpoint: "/v1/chat/completions",
    completion_window: "24h",
  });

  await db
    .update(ocrJobs)
    .set({
      batchId: batch.id,
      batchInputFileId: inputFile.id,
    })
    .where(eq(ocrJobs.jobId, jobId));

  return {
    batchId: batch.id,
    batchInputFileId: inputFile.id,
  };
};

const waitForBatchCompletion = async ({
  jobId,
  batchId,
  sleep,
  openai,
}: {
  jobId: string;
  batchId: string;
  sleep: SleepFn;
  openai: OpenAI;
}): Promise<string> => {
  let attempt = 0;
  while (true) {
    const latestBatch = await openai.batches.retrieve(batchId);

    if (
      latestBatch.status === "completed" &&
      latestBatch.output_file_id
    ) {
      return latestBatch.output_file_id as string;
    }

    if (
      latestBatch.status === "failed" ||
      latestBatch.status === "cancelled"
    ) {
      throw new Error(`Batch failed with status=${latestBatch.status}`);
    }

    await sleep(
      `${OcrSleepId.WaitBatchCompletion}-${jobId}-${attempt}`,
      BATCH_SLEEP_INTERVAL
    );
    attempt += 1;
  }
};

const saveBatchResults = async ({
  jobId,
  batchOutputFileId,
  totalImages,
  openai,
}: {
  jobId: string;
  batchOutputFileId: string;
  totalImages: number;
  openai: OpenAI;
}) => {
  const outputStream = await openai.files.content(batchOutputFileId);
  const outputBuffer = Buffer.from(await outputStream.arrayBuffer());
  const outputJsonl = outputBuffer.toString("utf8");

  const lines = outputJsonl
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  if (!lines.length) {
    throw new Error("Batch output file is empty.");
  }

  if (totalImages > 0 && lines.length !== totalImages) {
    throw new Error(
      `Batch output mismatch: expected ${totalImages} responses but got ${lines.length}.`
    );
  }

  const framesToPersist: PersistableFrame[] = [];

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
        `Invalid JSON line in batch output: ${(error as Error).message}`
      );
    }

    if (parsed.error) {
      const message =
        parsed.error?.message ??
        parsed.error?.code ??
        "Unknown OpenAI batch error";
      throw new Error(
        `OpenAI batch entry failed (${parsed.custom_id ?? "unknown"}): ${message}`
      );
    }

    const customId = parsed.custom_id;
    if (!customId) {
      continue;
    }

    const match = customId.match(/^job-(.+)-frame-(\d+)-(.+)$/);
    if (!match) {
      continue;
    }

    const [, , indexAsString, filename] = match;
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
  }

  if (!framesToPersist.length) {
    throw new Error("No OCR frames were parsed from the batch output.");
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
}: {
  jobId: string;
  paths: WorkspacePaths;
  storageKeys: StorageKeys;
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
    paths.batchJsonlPath,
    paths.txtPath,
    paths.docxPath,
    paths.rawArchivePath,
  ];

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
      let batchId = job.batchId ?? null;
      let batchInputFileId = job.batchInputFileId ?? null;
      let batchOutputFileId = job.batchOutputFileId ?? null;

      const workspacePaths = buildWorkspacePaths(jobId);
      const storageKeys = buildStorageKeys(jobId);
      let rawZipKeyForJob: string | null = job.rawZipPath ?? null;

      await ensureWorkspaceLayout(workspacePaths);

      const streamingResult = await step.run(
        OcrStepId.PreprocessImagesAndCrops,
        () =>
          streamAndProcessZip({
            jobId,
            zipKey: storageZipKey,
            storageKeys,
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

      const cropsMeta: CropMeta[] = streamingResult.cropsMeta;
      if (!cropsMeta.length) {
        throw new Error("No crops were generated from the provided ZIP file.");
      }
      currentStep = JobStep.BATCH_SUBMITTED;

      if (currentStep === JobStep.BATCH_SUBMITTED) {
        if (!batchId || !batchInputFileId) {
          const artifacts = await step.run(
            OcrStepId.CreateAndAwaitBatch,
            () =>
              createBatchArtifacts({
                jobId,
                cropsMeta,
                paths: workspacePaths,
                openai,
              })
          );
          batchId = artifacts.batchId;
          batchInputFileId = artifacts.batchInputFileId;
        }

        if (!batchId) {
          throw new Error("Batch ID missing after creation.");
        }

        batchOutputFileId = await waitForBatchCompletion({
          jobId,
          batchId,
          sleep: step.sleep,
          openai,
        });

        await db
          .update(ocrJobs)
          .set({
            batchOutputFileId,
            step: JobStep.RESULTS_SAVED,
          })
          .where(eq(ocrJobs.jobId, jobId));

        currentStep = JobStep.RESULTS_SAVED;
      }

      if (!batchOutputFileId) {
        batchOutputFileId = job.batchOutputFileId ?? null;
      }

      if (currentStep === JobStep.RESULTS_SAVED) {
        if (!batchOutputFileId) {
          throw new Error("Batch output file id missing");
        }

        await step.run(OcrStepId.SaveResultsToDb, () =>
          saveBatchResults({
            jobId,
            batchOutputFileId,
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