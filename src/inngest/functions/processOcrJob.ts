import * as fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import archiver from "archiver";
import { Transform } from "node:stream";
import { inngest } from "@/inngest/client";
import type { SelectOcrJob } from "@/db/schema/jobs";
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
import { JobItemType } from "@/types/enums/jobs/jobItemType.enum";
import { JobType } from "@/types/enums/jobs/jobType.enum";
import { OcrBatchKind } from "@/types/enums/jobs/ocrBatchKind.enum";
import { OcrBatchStatus } from "@/types/enums/jobs/ocrBatchStatus.enum";
import { OcrCropStatus } from "@/types/enums/jobs/ocrCropStatus.enum";
import { InngestEvents, OcrStepId, OcrSleepId } from "@/types/enums/inngest";
import { getUserOpenAIClient } from "@/utils/openai-user";
import type { OpenAI } from "openai";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { AI_CONSTANTS } from "@/constants/ai.constants";
import {
  countCropsForJob,
  deleteJobItemByType,
  getCropsByIds,
  getCropsForBatchRange,
  getCropsForJob,
  getFramesForJob,
  getInFlightBatch,
  getJobById,
  getJobItemByType,
  getJobItemMetaByType,
  getLastBatchSummary,
  getNextBatchNo,
  getPendingCrops,
  insertBatch,
  persistProgress,
  replaceCropsForJob,
  updateBatch,
  updateCropsFailedRetryable,
  updateCropsStatus,
  updateJob,
  updateJobItemSizeByType,
  upsertFrames,
  upsertJobItem,
  type OcrJobCropRow,
  type PersistableFrame,
} from "@/repositories/ocrJobRepository";
import {
  INITIAL_BACKOFF_MS,
  MAX_BACKOFF_MS,
  adjustBatchSizeOnTokenError,
  describeError,
  isRateLimitError,
  isServerError,
  isTokenLimitError,
} from "./batchHelpers";
import {
  getJobDocxKey,
  getJobOriginalZipKey,
  getJobTxtKey,
  getJobCropKey,
  getJobThumbnailKey,
  uploadFileToObject,
  uploadBufferToObject,
  uploadStreamToObject,
  createSignedDownloadUrlWithTtl,
  downloadObjectToTempFileVerified,
  withTempFileFromR2,
  getObjectSize,
  deleteObjectsByPrefix,
  deleteObjectIfExists,
} from "@/utils/storage";
import { forEachZipEntry, readStreamToBuffer } from "@/utils/zip/zipFile";
import { env } from "@/config/env.config";

const BATCH_SLEEP_INTERVAL = "20s";
const BATCH_SIZE = AI_CONSTANTS.BATCH.DEFAULT_SIZE;

type JobCrop = OcrJobCropRow;

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
  originalZipKey: string;
};

type StreamingArtifacts = {
  totalImages: number;
  originalZipKey: string | null;
  originalZipSizeBytes: number | null;
  thumbnailKey: string | null;
  crops: Array<Pick<JobCrop, "filename" | "baseKey" | "cropKey">>;
};


const sleepMs = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const assertR2ObjectMatchesExpectedSize = async (params: {
  key: string;
  expectedSizeBytes: number | null;
  context: string;
}): Promise<number> => {
  const actualSizeBytes = await getObjectSize(params.key);
  if (actualSizeBytes === null) {
    throw new Error(
      `R2 object not found for ${params.context}. key="${params.key}"`
    );
  }

  if (
    params.expectedSizeBytes !== null &&
    actualSizeBytes !== params.expectedSizeBytes
  ) {
    throw new Error(
      `R2 object size mismatch for ${params.context}. key="${params.key}", expectedSizeBytes=${params.expectedSizeBytes}, actualSizeBytes=${actualSizeBytes}. ` +
        "This usually means the uploaded ZIP is truncated/corrupt (common after an incorrect multipart upload implementation)."
    );
  }

  return actualSizeBytes;
};

const streamAndProcessZip = async ({
  userId,
  jobId,
  zipKey,
  storageKeys,
  onPreprocessProgress,
}: {
  userId: string;
  jobId: string;
  zipKey: string;
  storageKeys: StorageKeys;
  onPreprocessProgress?: (count: number) => Promise<void>;
}): Promise<StreamingArtifacts> => {
  const { tempPath, release } = await downloadObjectToTempFileVerified({
    key: zipKey,
    prefix: `ocr-${jobId}`,
  });
  // Use yauzl instead of unzipper for large/ZIP64 robustness.

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
    key: storageKeys.originalZipKey,
    stream: archiveOutput,
    contentType: "application/zip",
  });

  const crops: Array<Pick<JobCrop, "filename" | "baseKey" | "cropKey">> = [];
  let processedImages = 0;
  let thumbnailKey: string | null = null;
  let failedEntries = 0;
  const failedEntryNames: string[] = [];

  try {
    await forEachZipEntry({
      filePath: tempPath,
      onEntry: async (entry) => {
        if (entry.isDirectory) return;

        const entryName = entry.fileName;
        const processable = validateProcessableImageEntry(entryName);
        if (!processable) return;

        try {
          const fileStream = await entry.openReadStream();
          const fileBuffer = await readStreamToBuffer(fileStream, {
            expectedBytes: entry.uncompressedSize,
            context: `entry="${entry.fileName}"`,
          });

          const normalizedBuffer = await normalizeBufferTo1280x720(fileBuffer);
          const cropBuffer = await cropSubtitleFromBuffer(normalizedBuffer);

          // Only include base images (1, 2, 3, etc.) in the final ZIP
          // Skip decimal variants (1.1, 1.2, etc.) from the ZIP
          // Use original image (raw) with original extension
          if (processable.shouldIncludeInZip) {
            // Extract original extension from originalName (e.g., "1.jpg" -> ".jpg")
            const originalExt =
              processable.originalName.match(/\.(png|jpe?g)$/i)?.[0] || ".png";
            const zipFilename = `${processable.baseName}${originalExt}`;
            archive.append(fileBuffer, { name: zipFilename });
          }

          // Create crop for ALL images (including 1.1, 1.2, etc.) for OCR processing
          // Use original filename to preserve the relationship
          const cropFilename = processable.originalName.replace(
            /\.(png|jpe?g)$/i,
            ".png"
          );
          const cropKey = getJobCropKey(userId, jobId, cropFilename);
          await uploadBufferToObject({
            key: cropKey,
            body: cropBuffer,
            contentType: "image/png",
          });

          crops.push({
            filename: cropFilename,
            baseKey: getBaseKeyFromFilename(cropFilename),
            cropKey,
          });

          if (!thumbnailKey) {
            const thumbnailBuffer =
              await createThumbnailFromBuffer(normalizedBuffer);
            const thumbKey = getJobThumbnailKey(userId, jobId);
            await uploadBufferToObject({
              key: thumbKey,
              body: thumbnailBuffer,
              contentType: "image/jpeg",
              cacheControl: "public, max-age=31536000, immutable",
            });
            thumbnailKey = thumbKey;
          }

          processedImages += 1;
          if (onPreprocessProgress && processedImages % 50 === 0) {
            await onPreprocessProgress(processedImages);
          }
        } catch (err) {
          failedEntries += 1;
          if (failedEntryNames.length < 20) {
            failedEntryNames.push(entry.fileName);
          }
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(
            `[zip-entry] skipping corrupt entry jobId=${jobId} entry="${entry.fileName}" uncompressedSize=${entry.uncompressedSize}: ${msg}`
          );
          // Best-effort: continue processing remaining entries.
          return;
        }
      },
    });
  } catch (err) {
    const sizeBytes = await getObjectSize(zipKey).catch(() => null);
    throw new Error(
      `Failed to iterate ZIP entries from spooled temp file. key="${zipKey}", sizeBytes=${sizeBytes ?? "unknown"}, tempPath="${tempPath}". ` +
        "This means the ZIP could not be read/iterated reliably (bad ZIP structure or I/O issue). " +
        "If you are using multipart uploads, verify all parts are present and completion happened only after all bytes were uploaded.",
      { cause: err }
    );
  } finally {
    if (env.R2_SPOOL_KEEP_FILES_ON_ERROR) {
      // Keep for inspection; do not delete.
      console.warn(`[zip-spool] keeping temp zip for inspection: ${tempPath}`);
    } else {
      await fs.unlink(tempPath).catch(() => undefined);
    }
    release();
  }

  if (failedEntries > 0) {
    console.warn(
      `[zip-entry] jobId=${jobId} skippedEntries=${failedEntries} examples=${failedEntryNames.join(
        ", "
      )}`
    );
  }

  await archive.finalize();
  await filteredZipUploadPromise;

  if (onPreprocessProgress) {
    await onPreprocessProgress(processedImages);
  }

  return {
    totalImages: processedImages,
    originalZipKey: processedImages > 0 ? storageKeys.originalZipKey : null,
    originalZipSizeBytes: processedImages > 0 ? filteredZipSizeBytes : null,
    thumbnailKey,
    crops: [...crops].sort((a, b) => compareImageFilenames(a.filename, b.filename)),
  };
};

// Solo cuenta cuántas imágenes procesables hay en el ZIP (sin descargar contenido)
const countProcessableImagesInZip = async (zipKey: string): Promise<number> => {
  return withTempFileFromR2({
    key: zipKey,
    prefix: "count",
    fn: async (tempPath) => {
      let total = 0;
      await forEachZipEntry({
        filePath: tempPath,
        onEntry: async (entry) => {
          if (entry.isDirectory) return;
          const entryName = entry.fileName;
          const processable = validateProcessableImageEntry(entryName);
          if (processable) total += 1;
        },
      });
      return total;
    },
  });
};

const cropSignedUrlTtlSeconds = env.CROP_SIGNED_URL_TTL_SECONDS ?? 60 * 60 * 24;

const buildCropCustomId = (cropId: string): string => `crop-${cropId}`;

const parseCropIdFromCustomId = (customId: string): string | null => {
  if (!customId.startsWith("crop-")) return null;
  const id = customId.slice("crop-".length);
  return id.length > 0 ? id : null;
};

type BatchOutputLine = {
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

const createAndSubmitOpenAiBatch = async (params: {
  jobId: string;
  batchNo: number;
  crops: JobCrop[];
  openai: OpenAI;
}): Promise<{ inputFileId: string; batchId: string; jsonlPath: string }> => {
  const jsonlPath = getJobBatchJsonlPath(params.jobId, params.batchNo);
  const jsonlStream = fsSync.createWriteStream(jsonlPath, { encoding: "utf8" });

  const streamPromise = new Promise<void>((resolve, reject) => {
    jsonlStream.on("error", (err) => reject(err));
    jsonlStream.on("finish", () => resolve());
  });

  for (const crop of params.crops) {
    const signed = await createSignedDownloadUrlWithTtl({
      key: crop.cropKey,
      responseContentType: "image/png",
      downloadFilename: crop.filename,
      ttlSeconds: cropSignedUrlTtlSeconds,
    });

    const line = {
      custom_id: buildCropCustomId(crop.ocrJobCropId),
      method: "POST",
      url: "/v1/chat/completions",
      body: {
        model: AI_CONSTANTS.MODELS.OPENAI,
        temperature: 0,
        max_tokens: 1024,
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: AI_CONSTANTS.PROMPTS.OCR },
              { type: "image_url", image_url: { url: signed.url } },
            ],
          },
        ],
      },
    };
    jsonlStream.write(`${JSON.stringify(line)}\n`);
  }

  jsonlStream.end();
  await streamPromise;

  const inputFile = await params.openai.files.create({
    file: fsSync.createReadStream(jsonlPath),
    purpose: "batch",
  });

  const batch = await params.openai.batches.create({
    input_file_id: inputFile.id,
    endpoint: "/v1/chat/completions",
    completion_window: "24h",
  });

  return { inputFileId: inputFile.id, batchId: batch.id, jsonlPath };
};

const getBatchStatus = async (params: {
  openai: OpenAI;
  batchId: string;
}): Promise<{
  status: string;
  outputFileId: string | null;
  errorFileId: string | null;
  failureReason: string | null;
}> => {
  const latestBatch = await params.openai.batches.retrieve(params.batchId);
  const failureReason =
    (latestBatch as unknown as { status_details?: { error?: { message?: string } } })
      .status_details?.error?.message ??
    (latestBatch as unknown as { last_error?: { message?: string } }).last_error
      ?.message ??
    null;
  return {
    status: latestBatch.status,
    outputFileId: latestBatch.output_file_id as string | null,
    errorFileId: (latestBatch as unknown as { error_file_id?: string | null })
      .error_file_id ?? null,
    failureReason,
  };
};

const isTokenLimitFailure = (message: string): boolean =>
  message.includes("Enqueued token limit") ||
  message.includes("token limit reached") ||
  message.includes("enqueued tokens") ||
  message.toLowerCase().includes("enqueued token");

const getBatchFailureMessage = async (params: {
  openai: OpenAI;
  errorFileId: string | null;
  failureReason: string | null;
}): Promise<string> => {
  if (params.failureReason) return params.failureReason;
  if (!params.errorFileId) return "";
  try {
    const stream = await params.openai.files.content(params.errorFileId);
    const buf = Buffer.from(await stream.arrayBuffer());
    const text = buf.toString("utf8");
    const first = text
      .split("\n")
      .map((l) => l.trim())
      .find((l) => l.length > 0);
    return first ?? text.slice(0, 500);
  } catch {
    return "";
  }
};

const parseBatchOutputAndPersist = async (params: {
  jobId: string;
  openai: OpenAI;
  outputFileId: string;
  crops: JobCrop[];
}): Promise<{ failedCropIds: string[]; processedCount: number }> => {
  const cropById = new Map(params.crops.map((c) => [c.ocrJobCropId, c]));
  const expected = new Set(params.crops.map((c) => c.ocrJobCropId));
  const seen = new Set<string>();
  const succeeded = new Set<string>();

  const outputStream = await params.openai.files.content(params.outputFileId);
  const outputBuffer = Buffer.from(await outputStream.arrayBuffer());
  const outputJsonl = outputBuffer.toString("utf8");

  const lines = outputJsonl
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  if (!lines.length) {
    throw new Error(`Batch output file is empty for job ${params.jobId}.`);
  }

  const frames: PersistableFrame[] = [];
  const failed: Array<{ cropId: string; message: string }> = [];

  for (const line of lines) {
    let parsed: BatchOutputLine;
    try {
      parsed = JSON.parse(line) as BatchOutputLine;
    } catch (error) {
      throw new Error(`Invalid JSON line in batch output: ${(error as Error).message}`);
    }

    const cropId = parsed.custom_id ? parseCropIdFromCustomId(parsed.custom_id) : null;
    if (!cropId || !expected.has(cropId)) continue;
    seen.add(cropId);

    if (parsed.error) {
      failed.push({ cropId, message: parsed.error.message ?? "Unknown item error" });
      continue;
    }

    const crop = cropById.get(cropId);
    if (!crop) continue;

    const completion = parsed.response?.body?.choices?.[0]?.message?.content;
    const text = extractTextFromCompletion(completion);
    const finalText = !text || text === "<EMPTY>" ? "" : text;

    frames.push({
      jobId: params.jobId,
      filename: crop.filename,
      baseKey: crop.baseKey,
      index: crop.index,
      text: finalText,
    });
    succeeded.add(cropId);
  }

  // Missing outputs are retryable failures.
  for (const cropId of expected) {
    if (!seen.has(cropId)) {
      failed.push({ cropId, message: "Missing output line for crop in batch output" });
    }
  }

  await upsertFrames(frames);

  const succeededIds = Array.from(succeeded);

  if (succeededIds.length) {
    await updateCropsStatus({
      cropIds: succeededIds,
      status: OcrCropStatus.PROCESSED,
      lastError: null,
    });
  }

  const failedIds = Array.from(new Set(failed.map((f) => f.cropId)));
  if (failedIds.length) {
    await updateCropsFailedRetryable(failed);
  }

  return { failedCropIds: failedIds, processedCount: frames.length };
};

const buildDocuments = async ({
  jobId,
  paths,
  storageKeys,
}: {
  jobId: string;
  paths: WorkspacePaths;
  storageKeys: StorageKeys;
}): Promise<void> => {
  const frames = await getFramesForJob(jobId);

  const paragraphs = buildParagraphsFromFrames(frames);
  // If OCR produced no text at all, still generate empty artifacts (do not fail the job).
  const safeParagraphs = paragraphs.length ? paragraphs : [""];

  const paragraphsWithBlankLine = safeParagraphs.flatMap((paragraph, index) =>
    index < safeParagraphs.length - 1 ? [paragraph, ""] : [paragraph]
  );

  const txtContent = paragraphsWithBlankLine.join("\n");

  await fs.writeFile(paths.txtPath, txtContent, "utf8");
  await writeDocxFromParagraphs(safeParagraphs, paths.docxPath);

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

  // Create items for documents
  await upsertJobItem({
    jobId,
    itemType: JobItemType.TXT_DOCUMENT,
    storageKey: storageKeys.txtKey,
    sizeBytes: txtStats.size,
    contentType: "text/plain; charset=utf-8",
  });

  await upsertJobItem({
    jobId,
    itemType: JobItemType.DOCX_DOCUMENT,
    storageKey: storageKeys.docxKey,
    sizeBytes: docxStats.size,
    contentType:
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  });

  // Update job status
  await updateJob(jobId, { status: JobsStatus.DONE });

  const dirsToRemove = [paths.rawDir, paths.normalizedDir, paths.cropsDir];
  const filesToRemove = [
    paths.zipPath,
    paths.txtPath,
    paths.docxPath,
    paths.rawArchivePath,
    path.join(paths.cropsDir, "cropsMeta.json"),
  ];

  // Remove all JSONL temp files for this job (prefix-based; supports resumable batchNo naming).
  try {
    const entries = await fs.readdir(VOLUME_DIRS.tmpBase);
    const prefix = `${jobId}-ocr`;
    await Promise.all(
      entries
        .filter((name) => name.startsWith(prefix) && name.endsWith(".jsonl"))
        .map((name) =>
          fs.unlink(path.join(VOLUME_DIRS.tmpBase, name)).catch(() => undefined)
        )
    );
  } catch {
    // ignore
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

  // Documents are now saved as items, no need to return anything
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

const buildStorageKeys = (userId: string, jobId: string): StorageKeys => ({
  txtKey: getJobTxtKey(userId, jobId),
  docxKey: getJobDocxKey(userId, jobId),
  originalZipKey: getJobOriginalZipKey(userId, jobId),
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

type ProgressState = {
  totalImages: number;
  processedImages: number; // completadas en preprocesado (crops/resize)
  submittedImages: number; // enviadas a OpenAI
  totalBatches: number;
  batchesCompleted: number;
};

const buildProgress = (overrides?: Partial<ProgressState>): ProgressState => ({
  totalImages: 0,
  processedImages: 0,
  submittedImages: 0,
  totalBatches: 0,
  batchesCompleted: 0,
  ...overrides,
});

// --- Helpers de flujo ---


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
    let job: SelectOcrJob | null = null;
    let currentStep: JobStep | null = null;

    const { jobId, zipKey, userId } = event.data as {
      jobId: string;
      zipKey: string;
      userId: string;
    };

    if (!userId) {
      console.error("UserId missing in event data", event.data);
      
      try {
        await updateJob(jobId, {
          status: JobsStatus.ERROR,
          error: "UserId missing in event data",
        });
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

      job = await getJobById(jobId);

      let progress = buildProgress({
        totalImages: job?.totalImages ?? 0,
        processedImages: job?.processedImages ?? 0,
        submittedImages: job?.submittedImages ?? 0,
        totalBatches: job?.totalBatches ?? 0,
        batchesCompleted: job?.batchesCompleted ?? 0,
      });

      if (!job) {
        console.error("Job not found", jobId);
        return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
      }

      // Validate that this is an OCR job, not a subtitle removal job
      if (job.jobType !== JobType.OCR) {
        console.error(
          `Job ${jobId} is not an OCR job. Job type: ${job.jobType}. This function only processes OCR jobs.`
        );
        return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
      }

      // RAW_ZIP = input zip (uploaded). The event zipKey is expected to be that input.
      const existingRawZipKey = await getJobItemByType(jobId, JobItemType.RAW_ZIP);
      const storageZipKey = zipKey ?? existingRawZipKey;
      if (!storageZipKey) {
        console.error("Zip key missing for job", jobId);
        return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
      }

      // Ensure RAW_ZIP item exists (input.zip)
      await upsertJobItem({
        jobId,
        itemType: JobItemType.RAW_ZIP,
        storageKey: storageZipKey,
        contentType: "application/zip",
      });

      // Validate the input ZIP in R2 and persist its size (helps detect truncated/corrupt uploads early)
      const rawZipMeta = await getJobItemMetaByType(jobId, JobItemType.RAW_ZIP);
      const actualRawZipSizeBytes = await assertR2ObjectMatchesExpectedSize({
        key: storageZipKey,
        expectedSizeBytes: rawZipMeta?.sizeBytes ?? null,
        context: `job ${jobId} RAW_ZIP`,
      });

      if (rawZipMeta && rawZipMeta.sizeBytes === null) {
        await updateJobItemSizeByType(
          jobId,
          JobItemType.RAW_ZIP,
          actualRawZipSizeBytes
        );
      }

      // Estado actual en memoria (se irá actualizando manualmente)
      currentStep = job.step ?? JobStep.PREPROCESSING;
      let totalImages = job.totalImages ?? 0;

      const workspacePaths = buildWorkspacePaths(jobId);
      const storageKeys = buildStorageKeys(userId, jobId);
      const originalZipKeyForJob: string | null = await getJobItemByType(
        jobId,
        JobItemType.ORIGINAL_ZIP
      );

      // If already completed, short-circuit (avoid reading missing temp files)
      if (job.step === JobStep.DOCS_BUILT || job.step === JobStep.RESULTS_SAVED) {
        const txtKey = await getJobItemByType(jobId, JobItemType.TXT_DOCUMENT);
        const docxKey = await getJobItemByType(jobId, JobItemType.DOCX_DOCUMENT);
        const originalZipKey = await getJobItemByType(jobId, JobItemType.ORIGINAL_ZIP);
        
        if (txtKey && docxKey) {
          return {
            jobId,
            txtKey,
            docxKey,
            rawZipKey: originalZipKey,
          };
        }
      }

      await ensureWorkspaceLayout(workspacePaths);

      // --- PREPROCESS (idempotent) ---
      // If we already have crops persisted and an ORIGINAL_ZIP, we can skip preprocessing.
      const hasPreprocessed =
        (await countCropsForJob(jobId)) > 0 && Boolean(originalZipKeyForJob);

      if (!hasPreprocessed) {
        // Count images quickly for progress visibility
        const countedImages = await step.run("count-images", () =>
          countProcessableImagesInZip(storageZipKey)
        );
        totalImages = countedImages;

        progress = buildProgress({
          totalImages,
          processedImages: 0,
          submittedImages: 0,
          totalBatches: 0,
          batchesCompleted: 0,
        });

        await persistProgress(jobId, progress, {
          step: JobStep.PREPROCESSING,
          status: JobsStatus.PROCESSING,
        });

        const streamingResult = await step.run(
          OcrStepId.PreprocessImagesAndCrops,
          () =>
            streamAndProcessZip({
              userId,
              jobId,
              zipKey: storageZipKey,
              storageKeys,
              onPreprocessProgress: async (count) => {
                progress.processedImages = count;
                await persistProgress(jobId, progress, {
                  step: JobStep.PREPROCESSING,
                  status: JobsStatus.PROCESSING,
                });
              },
            })
        );

        totalImages = streamingResult.totalImages;

        // Persist crops (deterministic index order). If rerun, rebuild from scratch.
        await replaceCropsForJob(
          jobId,
          streamingResult.crops.map((c, idx) => ({
            index: idx,
            filename: c.filename,
            baseKey: c.baseKey,
            cropKey: c.cropKey,
            status: OcrCropStatus.UPLOADED,
          }))
        );

        progress = buildProgress({
          totalImages,
          processedImages: totalImages,
          submittedImages: 0,
          totalBatches: 0,
          batchesCompleted: 0,
        });

        await persistProgress(jobId, progress, {
          step: JobStep.BATCH_SUBMITTED,
          status: JobsStatus.PROCESSING,
        });

        // Create item for ORIGINAL_ZIP (final filtered zip) and thumbnail
        let originalZipItemId: string | undefined;
        if (streamingResult.originalZipKey) {
          originalZipItemId = await upsertJobItem({
            jobId,
            itemType: JobItemType.ORIGINAL_ZIP,
            storageKey: streamingResult.originalZipKey,
            sizeBytes: streamingResult.originalZipSizeBytes ?? undefined,
            contentType: "application/zip",
          });
        }

        if (streamingResult.thumbnailKey && originalZipItemId) {
          await upsertJobItem({
            jobId,
            itemType: JobItemType.THUMBNAIL,
            storageKey: streamingResult.thumbnailKey,
            contentType: "image/jpeg",
            parentItemId: originalZipItemId,
          });
        }
      }

      // Load crops from DB for batching
      const crops: JobCrop[] = await getCropsForJob(jobId);

      if (!crops.length) {
        throw new Error(`No crops found in DB for job ${jobId}. Preprocess may have failed.`);
      }

      // Quick preflight: ensure at least one crop exists in R2
      const firstCropSize = await getObjectSize(crops[0].cropKey);
      if (firstCropSize === null) {
        throw new Error(
          `Crops not found in storage for job ${jobId}. First missing key="${crops[0].cropKey}".`
        );
      }

      currentStep = JobStep.BATCH_SUBMITTED;

      if (currentStep === JobStep.BATCH_SUBMITTED) {
        const pendingStatuses: OcrCropStatus[] = [
          OcrCropStatus.UPLOADED,
          OcrCropStatus.FAILED_RETRYABLE,
        ];

        const waitForBatch = async (batchId: string, batchNo: number) => {
          let attempt = 0;
          while (true) {
            const st = await step.run(
              `${OcrStepId.WaitBatchCompletion}-${jobId}-batchNo-${batchNo}-attempt-${attempt}`,
              () => getBatchStatus({ openai, batchId })
            );

            if (st.status === "completed" && st.outputFileId) {
              return st;
            }

            if (st.status === "failed" || st.status === "cancelled") {
              const failureMessage = await getBatchFailureMessage({
                openai,
                errorFileId: st.errorFileId,
                failureReason: st.failureReason,
              });
              return { ...st, failureReason: failureMessage || st.failureReason };
            }

            await step.sleep(
              `${OcrSleepId.WaitBatchCompletion}-${jobId}-batchNo-${batchNo}-attempt-${attempt}`,
              BATCH_SLEEP_INTERVAL
            );
            attempt += 1;
          }
        };

        const runRetryPasses = async (params: {
          parentBatchId: string;
          failedCropIds: string[];
        }): Promise<void> => {
          const retryPassSizes = [100, 50, 20, 10];
          let pendingIds = Array.from(new Set(params.failedCropIds));

          for (let pass = 0; pass < retryPassSizes.length && pendingIds.length > 0; pass += 1) {
            const passSize = retryPassSizes[pass]!;
            const nextPending: string[] = [];

            for (let i = 0; i < pendingIds.length; i += passSize) {
              const sliceIds = pendingIds.slice(i, i + passSize);
              const sliceCrops = await getCropsByIds(sliceIds);

              if (!sliceCrops.length) continue;

              const batchNo = await getNextBatchNo(jobId);
              const startIndex = sliceCrops[0].index;
              const endIndexExclusive = sliceCrops[sliceCrops.length - 1].index + 1;

              const batchId = await insertBatch({
                jobId,
                batchNo,
                startIndex,
                endIndexExclusive,
                batchSize: sliceCrops.length,
                kind: OcrBatchKind.RETRY,
                parentBatchId: params.parentBatchId,
                status: OcrBatchStatus.CREATED,
                tokenLimitDetected: false,
              });

              try {
                const artifacts = await step.run(
                  `${OcrStepId.CreateAndAwaitBatch}-${jobId}-retry-batchNo-${batchNo}`,
                  () =>
                    createAndSubmitOpenAiBatch({
                      jobId,
                      batchNo,
                      crops: sliceCrops,
                      openai,
                    })
                );

                await updateBatch(batchId, {
                  openaiBatchId: artifacts.batchId,
                  openaiInputFileId: artifacts.inputFileId,
                  status: OcrBatchStatus.SUBMITTED,
                  failureReason: null,
                  tokenLimitDetected: false,
                });

                const st = await waitForBatch(artifacts.batchId, batchNo);

                if (st.status !== "completed" || !st.outputFileId) {
                  const msg = st.failureReason ?? "";
                  const tokenLimited = msg ? isTokenLimitFailure(msg) : false;
                  await updateBatch(batchId, {
                    status:
                      st.status === "cancelled"
                        ? OcrBatchStatus.CANCELLED
                        : OcrBatchStatus.FAILED,
                    failureReason: msg || null,
                    openaiErrorFileId: st.errorFileId,
                    tokenLimitDetected: tokenLimited,
                  });

                  // If token-limited even on retry, keep items pending for next (smaller) pass.
                  nextPending.push(...sliceIds);
                  continue;
                }

                const parsed = await parseBatchOutputAndPersist({
                  jobId,
                  openai,
                  outputFileId: st.outputFileId,
                  crops: sliceCrops,
                });

                await updateBatch(batchId, {
                  status: OcrBatchStatus.COMPLETED,
                  openaiOutputFileId: st.outputFileId,
                  openaiErrorFileId: st.errorFileId,
                  failureReason: null,
                });

                // Any failures remain for next pass.
                nextPending.push(...parsed.failedCropIds);
              } catch (error) {
                const tokenLimited = isTokenLimitError(error);
                await updateBatch(batchId, {
                  status: OcrBatchStatus.FAILED,
                  failureReason: describeError(error),
                  tokenLimitDetected: tokenLimited,
                });

                nextPending.push(...sliceIds);
              }
            }

            pendingIds = Array.from(new Set(nextPending));
          }

          // Anything still pending after retries becomes FAILED_FINAL with empty text
          if (pendingIds.length) {
            await updateCropsStatus({
              cropIds: pendingIds,
              status: OcrCropStatus.FAILED_FINAL,
            });

            const failedCrops = await getCropsByIds(pendingIds);

            await upsertFrames(
              failedCrops.map((c) => ({
                jobId,
                filename: c.filename,
                baseKey: c.baseKey,
                index: c.index,
                text: "",
              }))
            );
          }
        };

        // Initialize batch size hint from last token-limited batch (if any)
        const lastBatch = await getLastBatchSummary(jobId);

        let currentBatchSize = BATCH_SIZE;
        if (lastBatch?.tokenLimitDetected) {
          currentBatchSize = adjustBatchSizeOnTokenError(lastBatch.batchSize);
        }
        currentBatchSize = Math.max(AI_CONSTANTS.BATCH.MIN_SIZE, currentBatchSize);

        let backoffMs = INITIAL_BACKOFF_MS;

        while (true) {
          // Resume any in-flight batch first
          const inFlight = await getInFlightBatch(jobId, [
            OcrBatchStatus.SUBMITTED,
            OcrBatchStatus.RUNNING,
          ]);

          if (inFlight?.openaiBatchId) {
            await updateBatch(inFlight.ocrJobBatchId, { status: OcrBatchStatus.RUNNING });

            const batchCrops = await getCropsForBatchRange({
              jobId,
              startIndex: inFlight.startIndex,
              endIndexExclusive: inFlight.endIndexExclusive,
            });
            const st = await waitForBatch(inFlight.openaiBatchId, inFlight.batchNo);

            if (st.status !== "completed" || !st.outputFileId) {
              const msg = st.failureReason ?? "";
              const tokenLimited = msg ? isTokenLimitFailure(msg) : false;
              await updateBatch(inFlight.ocrJobBatchId, {
                status:
                  st.status === "cancelled"
                    ? OcrBatchStatus.CANCELLED
                    : OcrBatchStatus.FAILED,
                failureReason: msg || null,
                openaiErrorFileId: st.errorFileId,
                tokenLimitDetected: tokenLimited,
              });

              if (tokenLimited) {
                currentBatchSize = adjustBatchSizeOnTokenError(currentBatchSize);
                currentBatchSize = Math.max(AI_CONSTANTS.BATCH.MIN_SIZE, currentBatchSize);
                await step.sleep(
                  `${OcrSleepId.WaitBatchCompletion}-${jobId}-tokenLimitBackoff`,
                  "60s"
                );
                continue;
              }

              throw new Error(
                `Batch failed. jobId=${jobId} batchNo=${inFlight.batchNo} status=${st.status} reason="${msg || "unknown"}"`
              );
            }

            const parsed = await step.run(
              `${OcrStepId.SaveResultsToDb}-${jobId}-batchNo-${inFlight.batchNo}`,
              () =>
                parseBatchOutputAndPersist({
                  jobId,
                  openai,
                  outputFileId: st.outputFileId!,
                  crops: batchCrops,
                })
            );

            await updateBatch(inFlight.ocrJobBatchId, {
              status: OcrBatchStatus.COMPLETED,
              openaiOutputFileId: st.outputFileId,
              openaiErrorFileId: st.errorFileId,
              failureReason: null,
            });

            if (parsed.failedCropIds.length) {
              await runRetryPasses({
                parentBatchId: inFlight.ocrJobBatchId,
                failedCropIds: parsed.failedCropIds,
              });
            }

            progress.batchesCompleted += 1;
            await persistProgress(jobId, progress);
            continue;
          }

          const pendingCrops = await getPendingCrops({
            jobId,
            statuses: pendingStatuses,
            limit: currentBatchSize,
          });

          if (!pendingCrops.length) break;

          const batchNo = await getNextBatchNo(jobId);
          const startIndex = pendingCrops[0].index;
          const endIndexExclusive = pendingCrops[pendingCrops.length - 1].index + 1;

          const batchId = await insertBatch({
            jobId,
            batchNo,
            startIndex,
            endIndexExclusive,
            batchSize: pendingCrops.length,
            kind: OcrBatchKind.PRIMARY,
            status: OcrBatchStatus.CREATED,
            tokenLimitDetected: false,
          });

          try {
            const artifacts = await step.run(
              `${OcrStepId.CreateAndAwaitBatch}-${jobId}-batchNo-${batchNo}`,
              async () => {
                let attempt = 0;
                while (true) {
                  try {
                    const result = await createAndSubmitOpenAiBatch({
                      jobId,
                      batchNo,
                      crops: pendingCrops,
                      openai,
                    });
                    backoffMs = INITIAL_BACKOFF_MS;
                    return result;
                  } catch (error) {
                    if (isTokenLimitError(error)) {
                      throw error;
                    }
                    if (isRateLimitError(error) || isServerError(error)) {
                      const waitMs = backoffMs;
                      backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                      await sleepMs(waitMs);
                      attempt += 1;
                      continue;
                    }
                    throw error;
                  }
                }
              }
            );

            progress.submittedImages += pendingCrops.length;
            await persistProgress(jobId, progress);

            await updateBatch(batchId, {
              openaiBatchId: artifacts.batchId,
              openaiInputFileId: artifacts.inputFileId,
              status: OcrBatchStatus.SUBMITTED,
              failureReason: null,
              tokenLimitDetected: false,
            });

            const st = await waitForBatch(artifacts.batchId, batchNo);

            if (st.status !== "completed" || !st.outputFileId) {
              const msg = st.failureReason ?? "";
              const tokenLimited = msg ? isTokenLimitFailure(msg) : false;

              await updateBatch(batchId, {
                status:
                  st.status === "cancelled"
                    ? OcrBatchStatus.CANCELLED
                    : OcrBatchStatus.FAILED,
                failureReason: msg || null,
                openaiErrorFileId: st.errorFileId,
                tokenLimitDetected: tokenLimited,
              });

              if (tokenLimited) {
                currentBatchSize = adjustBatchSizeOnTokenError(currentBatchSize);
                currentBatchSize = Math.max(AI_CONSTANTS.BATCH.MIN_SIZE, currentBatchSize);
                await step.sleep(
                  `${OcrSleepId.WaitBatchCompletion}-${jobId}-batchNo-${batchNo}-tokenLimitBackoff`,
                  "60s"
                );
                continue;
              }

              throw new Error(
                `Batch failed. jobId=${jobId} batchNo=${batchNo} status=${st.status} reason="${msg || "unknown"}"`
              );
            }

            const parsed = await step.run(
              `${OcrStepId.SaveResultsToDb}-${jobId}-batchNo-${batchNo}`,
              () =>
                parseBatchOutputAndPersist({
                  jobId,
                  openai,
                  outputFileId: st.outputFileId!,
                  crops: pendingCrops,
                })
            );

            await updateBatch(batchId, {
              status: OcrBatchStatus.COMPLETED,
              openaiOutputFileId: st.outputFileId,
              openaiErrorFileId: st.errorFileId,
              failureReason: null,
            });

            if (parsed.failedCropIds.length) {
              await runRetryPasses({
                parentBatchId: batchId,
                failedCropIds: parsed.failedCropIds,
              });
            }

            progress.batchesCompleted += 1;
            await persistProgress(jobId, progress);
          } catch (error) {
            if (isTokenLimitError(error)) {
              await updateBatch(batchId, {
                status: OcrBatchStatus.FAILED,
                failureReason: describeError(error),
                tokenLimitDetected: true,
              });

              const next = adjustBatchSizeOnTokenError(currentBatchSize);
              if (next === currentBatchSize && currentBatchSize === AI_CONSTANTS.BATCH.MIN_SIZE) {
                throw new Error(
                  `Token limit at minimum batch size ${AI_CONSTANTS.BATCH.MIN_SIZE}. Last error: ${describeError(error)}`
                );
              }
              currentBatchSize = Math.max(AI_CONSTANTS.BATCH.MIN_SIZE, next);
              continue;
            }
            await updateBatch(batchId, {
              status: OcrBatchStatus.FAILED,
              failureReason: describeError(error),
            });
            throw error;
          }
        }

        await updateJob(jobId, { step: JobStep.RESULTS_SAVED });
        currentStep = JobStep.DOCS_BUILT;
      }

      if (currentStep === JobStep.DOCS_BUILT) {
        await step.run(OcrStepId.BuildDocsAndCleanup, () =>
          buildDocuments({
            jobId,
            paths: workspacePaths,
            storageKeys,
          })
        );

        // Cleanup storage: delete crops and delete RAW_ZIP (input)
        await deleteObjectsByPrefix(`users/${userId}/${jobId}/crops/`);
        const rawZipKey = await getJobItemByType(jobId, JobItemType.RAW_ZIP);
        if (rawZipKey) {
          await deleteObjectIfExists(rawZipKey).catch(() => undefined);
          await deleteJobItemByType(jobId, JobItemType.RAW_ZIP);
        }

        // Ensure the job reflects the final step in case any prior update was skipped
        await updateJob(jobId, {
          step: JobStep.DOCS_BUILT,
          status: JobsStatus.DONE,
        });
      }

      // Get final keys from items
      const finalTxtKey = await getJobItemByType(jobId, JobItemType.TXT_DOCUMENT);
      const finalDocxKey = await getJobItemByType(jobId, JobItemType.DOCX_DOCUMENT);
      const finalOriginalZipKey = await getJobItemByType(jobId, JobItemType.ORIGINAL_ZIP);

      return {
        jobId,
        txtKey: finalTxtKey ?? storageKeys.txtKey,
        docxKey: finalDocxKey ?? storageKeys.docxKey,
        rawZipKey: finalOriginalZipKey,
      };
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : "Unknown error in OCR job";
      const errorWithContext = `processOcrJob failed (jobId=${jobId}, step=${currentStep ?? job?.step ?? "unknown"}): ${errorMessage}`;

      console.error(errorWithContext, err);

      // Guardar error y marcar job como ERROR; el retry lo relanza desde el step que quedó
      await updateJob(jobId, {
        status: JobsStatus.ERROR,
        error: errorWithContext,
      });

      throw err;
    }
  }
);
