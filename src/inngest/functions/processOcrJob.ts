import * as fs from "node:fs/promises";
import fsSync from "node:fs";
import archiver from "archiver";
import { Transform } from "node:stream";
import { inngest } from "@/inngest/client";
import type { InngestFunction } from "inngest";
import type { SelectOcrJob } from "@/db/schema/jobs";
import {
  validateProcessableImageEntry,
  extractTextFromCompletion,
  normalizeBufferTo1280x720,
  cropSubtitleFromBuffer,
  createThumbnailFromBuffer,
  getBaseKeyFromFilename,
} from "@/utils/ocr";
import {
  getJobRootDir,
  getJobRawDir,
  getJobTxtPath,
  getJobDocxPath,
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
import { InngestEvents, OcrStepId } from "@/types/enums/inngest";
import { getUserOpenAIClient } from "@/utils/openai-user";
import type { OpenAI } from "openai";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { AI_CONSTANTS } from "@/constants/ai.constants";
import {
  getFramesForJob,
  getJobById,
  getJobItemByType,
  persistProgress,
  updateJob,
  upsertFrames,
  upsertJobItem,
  type PersistableFrame,
} from "@/repositories/ocrJobRepository";
import {
  getJobDocxKey,
  getJobOriginalZipKey,
  getJobTxtKey,
  getJobThumbnailKey,
  getJobZipKey,
  uploadFileToObject,
  uploadBufferToObject,
  uploadStreamToObject,
  downloadObjectToTempFileVerified,
  getObjectSize,
  deleteObjectIfExists,
} from "@/utils/storage";
import { forEachZipEntry, readStreamToBuffer } from "@/utils/zip/zipFile";
import { env } from "@/config/env.config";

// ============================================================================
// CONSTANTS
// ============================================================================

const PROCESSING_LOG_INTERVAL = 50; // Log progress every N images
const MAX_CROP_RETRIES = 3; // Maximum retries per crop
const INITIAL_BACKOFF_MS = 2000; // Initial backoff for retries
const MAX_BACKOFF_MS = 30000; // Maximum backoff for retries

// ============================================================================
// TYPES AND INTERFACES
// ============================================================================

type WorkspacePaths = {
  jobRootDir: string;
  rawDir: string;
  txtPath: string;
  docxPath: string;
  zipPath: string;
  rawArchivePath: string;
};

type StorageKeys = {
  txtKey: string;
  docxKey: string;
  originalZipKey: string;
};

type ZipProcessingResult = {
  frames: PersistableFrame[];
  totalImages: number;
  originalZipKey: string | null;
  originalZipSizeBytes: number | null;
  thumbnailKey: string | null;
};



// ============================================================================
// VALIDATION AND SETUP HELPERS
// ============================================================================

const validateJobAndUser = async (jobId: string, userId: string): Promise<SelectOcrJob> => {
  console.log(`[ocr-worker] [VALIDATE_JOB] Starting: jobId=${jobId}, userId=${userId}`);

  if (!userId) {
    throw new Error("UserId missing in event data");
  }

  const job = await getJobById(jobId);
  if (!job) {
    throw new Error(`Job not found: ${jobId}`);
  }

  if (job.jobType !== JobType.OCR) {
    throw new Error(`Job is not an OCR job: ${jobId}, jobType=${job.jobType}`);
  }

  console.log(`[ocr-worker] [VALIDATE_JOB] Success: jobId=${jobId}, status=${job.status}, step=${job.step}`);
  return job;
};

const ensureStorageSetup = async (zipKey: string, jobId: string): Promise<number> => {
  console.log(`[ocr-worker] [STORAGE_SETUP] Starting: jobId=${jobId}, zipKey=${zipKey}`);

  // Verify zipKey exists in R2
  const zipKeySize = await getObjectSize(zipKey);
  if (zipKeySize === null) {
    throw new Error(`ZIP file not found in R2: ${zipKey}`);
  }

  // Ensure RAW_ZIP item exists
  await upsertJobItem({
    jobId,
    itemType: JobItemType.RAW_ZIP,
    storageKey: zipKey,
    sizeBytes: zipKeySize,
    contentType: "application/zip",
  });

  console.log(`[ocr-worker] [STORAGE_SETUP] Success: jobId=${jobId}, zipKeySize=${zipKeySize}`);
  return zipKeySize;
};


// ============================================================================
// PREPROCESSING AND PROCESSING (COMBINED)
// ============================================================================

const bufferToBase64 = (buffer: Buffer): string => {
  return `data:image/png;base64,${buffer.toString("base64")}`;
};

const processCropBufferWithOpenAI = async (params: {
  cropBuffer: Buffer;
  filename: string;
  openai: OpenAI;
}): Promise<{ success: boolean; text: string; error?: string }> => {
  try {
    const base64Image = bufferToBase64(params.cropBuffer);

    const completion = await params.openai.chat.completions.create({
      model: AI_CONSTANTS.MODELS.OPENAI,
      temperature: 0,
      max_tokens: 1024,
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: AI_CONSTANTS.PROMPTS.OCR },
            {
              type: "image_url",
              image_url: { url: base64Image }
            },
          ],
        },
      ],
    });

    const text = extractTextFromCompletion(completion.choices[0]?.message?.content || undefined);
    const finalText = !text || text === "<EMPTY>" ? "" : text;

    return { success: true, text: finalText };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error(`[ocr-worker] [PROCESS_CROP] Error: filename=${params.filename}, error=${errorMsg}`, error);
    return { success: false, text: "", error: errorMsg };
  }
};

const processZipAndExtractText = async (params: {
  zipKey: string;
  userId: string;
  jobId: string;
  openai: OpenAI;
  onProgress?: (processed: number, total: number) => Promise<void>;
}): Promise<ZipProcessingResult> => {
  console.log(`[ocr-worker] [PROCESS_ZIP] Starting: jobId=${params.jobId}, zipKey=${params.zipKey}`);

  const { tempPath, release, sizeBytes } = await downloadObjectToTempFileVerified({
    key: params.zipKey,
    prefix: `ocr-${params.jobId}`,
  });

  console.log(`[ocr-worker] [PROCESS_ZIP] ZIP downloaded: jobId=${params.jobId}, sizeBytes=${sizeBytes}, tempPath=${tempPath}`);

  try {
    // First pass: count total processable images
    let totalImages = 0;
    await forEachZipEntry({
      filePath: tempPath,
      onEntry: async (entry) => {
        if (entry.isDirectory) return;
        const processable = validateProcessableImageEntry(entry.fileName);
        if (processable) {
          totalImages += 1;
        }
      },
    });

    console.log(`[ocr-worker] [PROCESS_ZIP] Total processable images: jobId=${params.jobId}, totalImages=${totalImages}`);

    // Save totalImages to DB immediately so frontend can show progress
    if (params.onProgress) {
      await params.onProgress(0, totalImages);
    }

    // Setup for creating ORIGINAL_ZIP
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
      key: getJobOriginalZipKey(params.userId, params.jobId),
      stream: archiveOutput,
      contentType: "application/zip",
    });

    const frames: PersistableFrame[] = [];
    let processedImages = 0;
    let thumbnailKey: string | null = null;
    let failedEntries = 0;

    console.log(`[ocr-worker] [PROCESS_ZIP] Starting image processing: jobId=${params.jobId}, totalImages=${totalImages}`);

    // Second pass: process each image
    await forEachZipEntry({
      filePath: tempPath,
      onEntry: async (entry) => {
        if (entry.isDirectory) return;

        const entryName = entry.fileName;
        const processable = validateProcessableImageEntry(entryName);
        if (!processable) return;

        try {
          // Read image buffer
          const fileStream = await entry.openReadStream();
          const fileBuffer = await readStreamToBuffer(fileStream, {
            expectedBytes: entry.uncompressedSize,
            context: `entry="${entry.fileName}"`,
          });

          // Normalize and crop
          const normalizedBuffer = await normalizeBufferTo1280x720(fileBuffer);
          const cropBuffer = await cropSubtitleFromBuffer(normalizedBuffer);

          // Add to ORIGINAL_ZIP if it's a base image
          if (processable.shouldIncludeInZip) {
            const originalExt = processable.originalName.match(/\.(png|jpe?g)$/i)?.[0] || ".png";
            const zipFilename = `${processable.baseName}${originalExt}`;
            archive.append(fileBuffer, { name: zipFilename });
          }

          // Process crop with OpenAI (with retries)
          let cropResult: { success: boolean; text: string; error?: string };
          let attempt = 0;
          let backoffMs = INITIAL_BACKOFF_MS;

          do {
            cropResult = await processCropBufferWithOpenAI({
              cropBuffer,
              filename: processable.originalName,
              openai: params.openai,
            });

            if (!cropResult.success && attempt < MAX_CROP_RETRIES) {
              console.warn(`[ocr-worker] [PROCESS_ZIP] Retry ${attempt + 1}/${MAX_CROP_RETRIES}: jobId=${params.jobId}, filename=${processable.originalName}`);
              await new Promise(resolve => setTimeout(resolve, backoffMs));
              backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
              attempt++;
            } else {
              break;
            }
          } while (attempt < MAX_CROP_RETRIES);

          // Insert frame immediately
          frames.push({
            jobId: params.jobId,
            filename: processable.originalName,
            baseKey: processable.baseName,
            index: processedImages,
            text: cropResult.success ? cropResult.text : "", // Empty text for failed crops
          });

          // Create thumbnail from first image
          if (!thumbnailKey) {
            const thumbnailBuffer = await createThumbnailFromBuffer(normalizedBuffer);
            const thumbKey = getJobThumbnailKey(params.userId, params.jobId);
            await uploadBufferToObject({
              key: thumbKey,
              body: thumbnailBuffer,
              contentType: "image/jpeg",
              cacheControl: "public, max-age=31536000, immutable",
            });
            thumbnailKey = thumbKey;
          }

          processedImages += 1;

          // Update progress after each image (for real-time UI updates)
          if (params.onProgress) {
            await params.onProgress(processedImages, totalImages);
          }

          // Log progress every N images
          if (processedImages % PROCESSING_LOG_INTERVAL === 0 || processedImages === totalImages) {
            console.log(`[ocr-worker] [PROCESS_ZIP] Progress: jobId=${params.jobId}, processed=${processedImages}/${totalImages} (${Math.round((processedImages / totalImages) * 100)}%)`);
          }

        } catch (error) {
          failedEntries += 1;
          const errorMsg = error instanceof Error ? error.message : String(error);
          console.error(`[ocr-worker] [PROCESS_ZIP] Failed entry: jobId=${params.jobId}, filename=${entryName}, error=${errorMsg}`, error);

          // Still insert frame with empty text for failed entries
          const processableForFailed = validateProcessableImageEntry(entryName);
          frames.push({
            jobId: params.jobId,
            filename: entryName,
            baseKey: processableForFailed?.baseName ?? entryName.replace(/\.(png|jpe?g)$/i, ''),
            index: processedImages,
            text: "",
          });
          processedImages += 1;

          // Update progress for failed entries too
          if (params.onProgress) {
            await params.onProgress(processedImages, totalImages);
          }
        }
      },
    });

    // Finalize archive
    await archive.finalize();
    await filteredZipUploadPromise;

    console.log(`[ocr-worker] [PROCESS_ZIP] Completed: jobId=${params.jobId}, processed=${processedImages}/${totalImages}, failed=${failedEntries}, frames=${frames.length}`);

    return {
      frames,
      totalImages,
      originalZipKey: processedImages > 0 ? getJobOriginalZipKey(params.userId, params.jobId) : null,
      originalZipSizeBytes: processedImages > 0 ? filteredZipSizeBytes : null,
      thumbnailKey,
    };

  } finally {
    if (env.R2_SPOOL_KEEP_FILES_ON_ERROR) {
      console.warn(`[ocr-worker] [PROCESS_ZIP] Keeping temp file for inspection: ${tempPath}`);
    } else {
      await fs.unlink(tempPath).catch(() => undefined);
    }
    release();
  }
};

// ============================================================================
// DOCUMENT BUILDING
// ============================================================================

const buildDocumentsFromFrames = async (params: {
  jobId: string;
  paths: WorkspacePaths;
  storageKeys: StorageKeys;
}): Promise<void> => {
  console.log(`[ocr-worker] [BUILD_DOCS] Starting: jobId=${params.jobId}`);

  const frames = await getFramesForJob(params.jobId);
  console.log(`[ocr-worker] [BUILD_DOCS] Loaded ${frames.length} frames: jobId=${params.jobId}`);
  
  // Debug: log frame grouping
  const frameGroups = new Map<string, string[]>();
  for (const frame of frames) {
    const baseKey = frame.baseKey && frame.baseKey.trim().length > 0
      ? frame.baseKey
      : getBaseKeyFromFilename(frame.filename);
    const group = frameGroups.get(baseKey) ?? [];
    group.push(`${frame.filename} (baseKey: ${baseKey})`);
    frameGroups.set(baseKey, group);
  }
  console.log(`[ocr-worker] [BUILD_DOCS] Frame groups: jobId=${params.jobId}`, 
    Array.from(frameGroups.entries()).map(([key, files]) => `${key}: [${files.join(', ')}]`).join(' | '));

  const paragraphs = buildParagraphsFromFrames(frames);
  console.log(`[ocr-worker] [BUILD_DOCS] Built ${paragraphs.length} paragraphs: jobId=${params.jobId}`);
  // If OCR produced no text at all, still generate empty artifacts (do not fail the job).
  const safeParagraphs = paragraphs.length ? paragraphs : [""];

  const paragraphsWithBlankLine = safeParagraphs.flatMap((paragraph, index) =>
    index < safeParagraphs.length - 1 ? [paragraph, ""] : [paragraph]
  );

  const txtContent = paragraphsWithBlankLine.join("\n");

  await fs.writeFile(params.paths.txtPath, txtContent, "utf8");
  await writeDocxFromParagraphs(safeParagraphs, params.paths.docxPath);

  // Calculate file sizes before uploading
  const txtStats = fsSync.statSync(params.paths.txtPath);
  const docxStats = fsSync.statSync(params.paths.docxPath);

  await uploadFileToObject({
    key: params.storageKeys.txtKey,
    filePath: params.paths.txtPath,
    contentType: "text/plain; charset=utf-8",
  });

  await uploadFileToObject({
    key: params.storageKeys.docxKey,
    filePath: params.paths.docxPath,
    contentType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  });

  // Create items for documents
  await upsertJobItem({
    jobId: params.jobId,
    itemType: JobItemType.TXT_DOCUMENT,
    storageKey: params.storageKeys.txtKey,
    sizeBytes: txtStats.size,
    contentType: "text/plain; charset=utf-8",
  });

  await upsertJobItem({
    jobId: params.jobId,
    itemType: JobItemType.DOCX_DOCUMENT,
    storageKey: params.storageKeys.docxKey,
    sizeBytes: docxStats.size,
    contentType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  });

  console.log(`[ocr-worker] [BUILD_DOCS] Completed: jobId=${params.jobId}, txtSize=${txtStats.size}, docxSize=${docxStats.size}`);
};

// ============================================================================
// PROGRESS AND STATUS HELPERS
// ============================================================================

const updateJobProgress = async (params: {
  jobId: string;
  step: JobStep;
  status: JobsStatus;
  processedImages: number;
  totalImages: number;
}): Promise<void> => {
  console.log(`[ocr-worker] [PROGRESS] Updating: jobId=${params.jobId}, step=${params.step}, status=${params.status}, processed=${params.processedImages}/${params.totalImages}`);

  await persistProgress(params.jobId, {
    totalImages: params.totalImages,
    processedImages: params.processedImages,
  }, {
    step: params.step,
    status: params.status,
  });
};

// ============================================================================
// CLEANUP AND UTILITIES
// ============================================================================

const cleanupJobArtifacts = async (params: {
  jobId: string;
  userId: string;
  paths: WorkspacePaths;
}): Promise<void> => {
  console.log(`[ocr-worker] [CLEANUP] Starting: jobId=${params.jobId}`);

  // Delete temporary files
  const filesToRemove = [
    params.paths.zipPath,
    params.paths.txtPath,
    params.paths.docxPath,
    params.paths.rawArchivePath,
  ];

  const dirsToRemove = [params.paths.rawDir];

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

  // Note: No crop cleanup needed since crops are processed directly from memory

  console.log(`[ocr-worker] [CLEANUP] Completed: jobId=${params.jobId}`);
};

const buildWorkspacePaths = (jobId: string): WorkspacePaths => ({
  jobRootDir: getJobRootDir(jobId),
  rawDir: getJobRawDir(jobId),
  txtPath: getJobTxtPath(jobId),
  docxPath: getJobDocxPath(jobId),
  zipPath: getJobZipPath(jobId),
  rawArchivePath: getJobRawArchivePath(jobId),
});

const buildStorageKeys = (userId: string, jobId: string): StorageKeys => ({
  txtKey: getJobTxtKey(userId, jobId),
  docxKey: getJobDocxKey(userId, jobId),
  originalZipKey: getJobOriginalZipKey(userId, jobId),
});

const ensureWorkspaceLayout = async (paths: WorkspacePaths): Promise<void> => {
  await fs.mkdir(paths.jobRootDir, { recursive: true });
  await fs.mkdir(paths.rawDir, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.txtBase, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.wordBase, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.tmpBase, { recursive: true });
};

// ============================================================================
// MAIN INNGEST FUNCTION
// ============================================================================

export const processOcrJob: InngestFunction.Any = inngest.createFunction(
  {
    id: InngestFunctions.PROCESS_OCR_JOB,
    triggers: [{ event: InngestEvents.ZIP_UPLOADED }],
    timeouts: {
      finish: "24h",
    },
  },
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

    console.log(`[ocr-worker] [MAIN] Starting OCR job: jobId=${jobId}, zipKey=${zipKey}, userId=${userId}`);

    let job: SelectOcrJob | null = null;

    try {
      // Step 1: Validate job and user
      job = await validateJobAndUser(jobId, userId);

      // Step 2: Get OpenAI client
      const openai = await getUserOpenAIClient(userId);

      // Step 3: Ensure storage setup
      await ensureStorageSetup(zipKey, jobId);

      // Step 4: Setup workspace
      const workspacePaths = buildWorkspacePaths(jobId);
      const storageKeys = buildStorageKeys(userId, jobId);
      await ensureWorkspaceLayout(workspacePaths);

      // Step 5: Check if already completed
      if (job.step === JobStep.DOCS_BUILT) {
        console.log(`[ocr-worker] [MAIN] Job already completed: jobId=${jobId}`);
        const txtKey = await getJobItemByType(jobId, JobItemType.TXT_DOCUMENT);
        const docxKey = await getJobItemByType(jobId, JobItemType.DOCX_DOCUMENT);
        const originalZipKey = await getJobItemByType(jobId, JobItemType.ORIGINAL_ZIP);

        return {
          jobId,
          txtKey: txtKey ?? storageKeys.txtKey,
          docxKey: docxKey ?? storageKeys.docxKey,
          rawZipKey: originalZipKey,
        };
      }

      // Step 6: Process ZIP and extract text (main processing)
      console.log(`[ocr-worker] [MAIN] Starting preprocessing/processing: jobId=${jobId}`);
      const processingResult = await step.run(
        OcrStepId.PreprocessImagesAndCrops,
        () => processZipAndExtractText({
          zipKey,
          userId,
          jobId,
          openai,
          onProgress: async (processed, total) => {
            await updateJobProgress({
              jobId,
              step: JobStep.PREPROCESSING,
              status: JobsStatus.PROCESSING,
              processedImages: processed,
              totalImages: total,
            });
          },
        })
      );

      // Step 7: Insert all frames at once
      if (processingResult.frames.length > 0) {
        console.log(`[ocr-worker] [MAIN] Inserting frames: jobId=${jobId}, frameCount=${processingResult.frames.length}`);
        await upsertFrames(processingResult.frames);
      }

      // Step 8: Create job items for ORIGINAL_ZIP and thumbnail
      if (processingResult.originalZipKey) {
        await upsertJobItem({
          jobId,
          itemType: JobItemType.ORIGINAL_ZIP,
          storageKey: processingResult.originalZipKey,
          sizeBytes: processingResult.originalZipSizeBytes ?? undefined,
          contentType: "application/zip",
        });
      }

      if (processingResult.thumbnailKey) {
        await upsertJobItem({
          jobId,
          itemType: JobItemType.THUMBNAIL,
          storageKey: processingResult.thumbnailKey,
          contentType: "image/jpeg",
        });
      }

      // Step 9: Update job progress to show completion
      await updateJobProgress({
        jobId,
        step: JobStep.PREPROCESSING,
        status: JobsStatus.PROCESSING,
        processedImages: processingResult.totalImages,
        totalImages: processingResult.totalImages,
      });

      // Step 10: Build documents
      console.log(`[ocr-worker] [MAIN] Building documents: jobId=${jobId}`);
      await step.run(OcrStepId.BuildDocsAndCleanup, () =>
        buildDocumentsFromFrames({
          jobId,
          paths: workspacePaths,
          storageKeys,
        })
      );

      // Step 11: Cleanup and finalize
      console.log(`[ocr-worker] [MAIN] Cleaning up: jobId=${jobId}`);
      await cleanupJobArtifacts({
        jobId,
        userId,
        paths: workspacePaths,
      });

      // Step 11.5: Delete raw zip (input.zip) from R2 and tmp after successful processing
      const rawZipKey = getJobZipKey(userId, jobId);
      try {
        console.log(`[ocr-worker] [MAIN] Deleting raw zip from R2: jobId=${jobId}, key=${rawZipKey}`);
        await deleteObjectIfExists(rawZipKey);
        console.log(`[ocr-worker] [MAIN] Successfully deleted raw zip from R2: jobId=${jobId}`);
      } catch (error) {
        // Log but don't fail the job if deletion fails
        console.warn(`[ocr-worker] [MAIN] Failed to delete raw zip from R2: jobId=${jobId}, error=${error instanceof Error ? error.message : String(error)}`);
      }

      // Raw zip from tmp is already deleted in cleanupJobArtifacts (zipPath)

      // Step 12: Mark job as done
      await updateJob(jobId, {
        step: JobStep.DOCS_BUILT,
        status: JobsStatus.DONE,
      });

      // Step 13: Get final keys
      const finalTxtKey = await getJobItemByType(jobId, JobItemType.TXT_DOCUMENT);
      const finalDocxKey = await getJobItemByType(jobId, JobItemType.DOCX_DOCUMENT);
      const finalOriginalZipKey = await getJobItemByType(jobId, JobItemType.ORIGINAL_ZIP);

      console.log(`[ocr-worker] [MAIN] Job completed successfully: jobId=${jobId}, totalImages=${processingResult.totalImages}`);

      return {
        jobId,
        txtKey: finalTxtKey ?? storageKeys.txtKey,
        docxKey: finalDocxKey ?? storageKeys.docxKey,
        rawZipKey: finalOriginalZipKey,
      };

    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error in OCR job";
      const errorWithContext = `[ocr-worker] [MAIN] Failed: jobId=${jobId}, step=${job?.step ?? "unknown"}, error=${errorMessage}`;

      console.error(errorWithContext, err);

      // Update job status to error
      await updateJob(jobId, {
        status: JobsStatus.ERROR,
        error: errorWithContext,
      });

      throw err;
    }
  }
);
