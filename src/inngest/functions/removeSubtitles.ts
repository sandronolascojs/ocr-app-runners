import * as fs from "node:fs/promises";
import archiver from "archiver";
import { Transform } from "node:stream";
import { env } from "@/config/env.config";
import { inngest } from "@/inngest/client";
import { db } from "@/db";
import { ocrJobs, ocrJobItems } from "@/db/schema";
import { eq, and } from "drizzle-orm";
import {
  validateProcessableImageEntry,
  createThumbnailFromBuffer,
} from "@/utils/ocr";
import { removeSubtitlesFromBuffer } from "@/utils/subtitles";
import {
  getJobRootDir,
  getJobRawDir,
  getJobNormalizedDir,
  VOLUME_DIRS,
} from "@/utils/paths";
import { JobsStatus } from "@/types";
import { JobItemType } from "@/types/enums/jobs/jobItemType.enum";
import { JobType } from "@/types/enums/jobs/jobType.enum";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { InngestEvents, OcrStepId } from "@/types/enums/inngest";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import {
  downloadObjectToTempFileVerified,
  getObjectSize,
  uploadStreamToObject,
  uploadBufferToObject,
  getJobCroppedZipKey,
  getJobCroppedThumbnailKey,
} from "@/utils/storage";
import { forEachZipEntry, readStreamToBuffer } from "@/utils/zip/zipFile";

type WorkspacePaths = {
  jobRootDir: string;
  rawDir: string;
  normalizedDir: string;
};

const buildWorkspacePaths = (jobId: string): WorkspacePaths => ({
  jobRootDir: getJobRootDir(jobId),
  rawDir: getJobRawDir(jobId),
  normalizedDir: getJobNormalizedDir(jobId),
});

const ensureWorkspaceLayout = async (paths: WorkspacePaths) => {
  await fs.mkdir(paths.jobRootDir, { recursive: true });
  await fs.mkdir(paths.rawDir, { recursive: true });
  await fs.mkdir(paths.normalizedDir, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.tmpBase, { recursive: true });
};

type JobItemMeta = {
  storageKey: string;
  sizeBytes: number | null;
};

/**
 * Helper function to get a job item meta by type
 */
const getJobItemMetaByType = async (
  jobId: string,
  itemType: JobItemType
): Promise<JobItemMeta | null> => {
  const [item] = await db
    .select({
      storageKey: ocrJobItems.storageKey,
      sizeBytes: ocrJobItems.sizeBytes,
    })
    .from(ocrJobItems)
    .where(
      and(eq(ocrJobItems.jobId, jobId), eq(ocrJobItems.itemType, itemType))
    )
    .limit(1);

  if (!item?.storageKey) return null;
  return { storageKey: item.storageKey, sizeBytes: item.sizeBytes ?? null };
};

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

/**
 * Helper function to persist progress to database
 */
const persistProgress = async (
  jobId: string,
  totalImages: number,
  processedImages: number
) => {
  await db
    .update(ocrJobs)
    .set({
      totalImages,
      processedImages,
      status: JobsStatus.PROCESSING,
    })
    .where(eq(ocrJobs.jobId, jobId));
};

/**
 * Count processable images in zip without downloading content
 */
const countProcessableImagesInZip = async (zipKey: string): Promise<number> => {
  const { tempPath, release } = await downloadObjectToTempFileVerified({
    key: zipKey,
    prefix: "subtitle-count",
  });
  let total = 0;
  try {
    await forEachZipEntry({
      filePath: tempPath,
      onEntry: async (entry) => {
        if (entry.isDirectory) return;
        const entryName = entry.fileName;
        const processable = validateProcessableImageEntry(entryName);
        if (processable) total += 1;
      },
    });
  } catch (err) {
    const sizeBytes = await getObjectSize(zipKey).catch(() => null);
    throw new Error(
      `Failed to parse ZIP from R2. key="${zipKey}", sizeBytes=${sizeBytes ?? "unknown"}. ` +
        "If you recently switched to multipart uploads, verify each part PUT uploads raw bytes (Blob.slice/ArrayBuffer) and not multipart/form-data, " +
        "and that the upload completes with all parts present.",
      { cause: err }
    );
  } finally {
    if (env.R2_SPOOL_KEEP_FILES_ON_ERROR) {
      console.warn(`[zip-spool] keeping temp zip for inspection: ${tempPath}`);
    } else {
      await fs.unlink(tempPath).catch(() => undefined);
    }
    release();
  }
  return total;
};

const streamAndRemoveSubtitles = async ({
  zipKey,
  storageKeys,
  onProgress,
}: {
  jobId: string;
  zipKey: string;
  storageKeys: { croppedZipKey: string; croppedThumbnailKey: string };
  onProgress?: (count: number) => Promise<void>;
}): Promise<{
  totalImages: number;
  croppedZipKey: string | null;
  croppedZipSizeBytes: number | null;
  croppedThumbnailKey: string | null;
}> => {
  const { tempPath, release } = await downloadObjectToTempFileVerified({
    key: zipKey,
    prefix: "subtitle-stream",
  });

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
    key: storageKeys.croppedZipKey,
    stream: archiveOutput,
    contentType: "application/zip",
  });

  let processedImages = 0;
  let firstImageBuffer: Buffer | null = null;

  try {
    await forEachZipEntry({
      filePath: tempPath,
      onEntry: async (entry) => {
        if (entry.isDirectory) return;

        const entryName = entry.fileName;
        const processable = validateProcessableImageEntry(entryName);
        if (!processable) return;

        const fileStream = await entry.openReadStream();
        const fileBuffer = await readStreamToBuffer(fileStream);
      // Remove subtitles from the original image buffer.
      // Important: do not normalize with "fit: contain" here, as it can introduce black borders.
      const imageWithoutSubtitles = await removeSubtitlesFromBuffer(fileBuffer);

      // Only include base images (1, 2, 3, etc.) in the final ZIP
      // Skip decimal variants (1.1, 1.2, etc.) from the ZIP
      if (processable.shouldIncludeInZip) {
        // Extract original extension from originalName (e.g., "1.jpg" -> ".jpg")
        const originalExt =
          processable.originalName.match(/\.(png|jpe?g)$/i)?.[0] || ".png";
        const zipFilename = `${processable.baseName}${originalExt}`;
        archive.append(imageWithoutSubtitles, { name: zipFilename });

        // Save first image for thumbnail generation
        if (!firstImageBuffer) {
          firstImageBuffer = imageWithoutSubtitles;
        }
      }

        processedImages += 1;
        if (onProgress && processedImages % 50 === 0) {
          await onProgress(processedImages);
        }
      },
    });
  } catch (err) {
    const sizeBytes = await getObjectSize(zipKey).catch(() => null);
    throw new Error(
      `Failed to stream ZIP entries from R2. key="${zipKey}", sizeBytes=${sizeBytes ?? "unknown"}. ` +
        "This indicates the ZIP is truncated/corrupt. If the ZIP was uploaded via multipart, ensure each part PUT is raw bytes (not multipart/form-data) " +
        "and that all parts are uploaded before completing the multipart upload.",
      { cause: err }
    );
  } finally {
    if (env.R2_SPOOL_KEEP_FILES_ON_ERROR) {
      console.warn(`[zip-spool] keeping temp zip for inspection: ${tempPath}`);
    } else {
      await fs.unlink(tempPath).catch(() => undefined);
    }
    release();
  }

  // Finalize archive first (this will trigger the end of the stream)
  // The sizeCounter Transform will continue counting until the stream ends
  await archive.finalize();
  
  // Wait for upload to complete - this ensures the file is fully uploaded and size is finalized
  await filteredZipUploadPromise;

  // Size should be finalized now after the stream has ended
  const finalSizeBytes = filteredZipSizeBytes;
  
  console.log(
    `Upload completed for cropped ZIP: ${storageKeys.croppedZipKey}, size: ${finalSizeBytes} bytes, processed images: ${processedImages}`
  );

  // Final progress update
  if (onProgress) {
    await onProgress(processedImages);
  }

  // Generate thumbnail from first cropped image
  let croppedThumbnailKey: string | null = null;
  if (firstImageBuffer && processedImages > 0) {
    const thumbnailBuffer = await createThumbnailFromBuffer(firstImageBuffer);
    await uploadBufferToObject({
      key: storageKeys.croppedThumbnailKey,
      body: thumbnailBuffer,
      contentType: "image/jpeg",
      cacheControl: "public, max-age=31536000, immutable",
    });
    croppedThumbnailKey = storageKeys.croppedThumbnailKey;
  }

  return {
    totalImages: processedImages,
    croppedZipKey: processedImages > 0 ? storageKeys.croppedZipKey : null,
    croppedZipSizeBytes: processedImages > 0 ? finalSizeBytes : null,
    croppedThumbnailKey,
  };
};

export const removeSubtitlesFromImages = inngest.createFunction(
  {
    id: InngestFunctions.REMOVE_SUBTITLES_FROM_IMAGES,
    timeouts: {
      finish: "2h",
    },
  },
  { event: InngestEvents.REMOVE_SUBTITLES },
  async ({ event, step }): Promise<{
    jobId: string;
    croppedZipKey: string | null;
  }> => {
    const { jobId, parentJobId, zipKey, userId } = event.data as {
      jobId: string;
      parentJobId?: string; // Parent OCR job ID (optional)
      zipKey?: string; // Direct zip key (optional)
      userId: string;
    };

    if (!userId) {
      console.error("UserId missing in event data", event.data);
      return { jobId: "", croppedZipKey: null };
    }

    if (!jobId) {
      console.error("JobId missing in event data", event.data);
      return { jobId: "", croppedZipKey: null };
    }

    // Validate that we have enough information to proceed
    // Need: parentJobId OR zipKey to know which zip to process
    if (!parentJobId && !zipKey) {
      console.error("Either parentJobId or zipKey must be provided", event.data);
      return { jobId: "", croppedZipKey: null };
    }

    try {
      let rawZipKey: string;
      let subtitleJobId: string;
      let subtitleJobUserId: string;
      let parentJobIdForSubtitle: string | null = null;

      // Determine the source zip and user
      if (parentJobId) {
        // Get the parent OCR job
        const [parentJob] = await db
          .select()
          .from(ocrJobs)
          .where(eq(ocrJobs.jobId, parentJobId))
          .limit(1);

        if (!parentJob) {
          console.error("Parent job not found", parentJobId);
          return { jobId: "", croppedZipKey: null };
        }

        // Validate ownership - only the job owner can remove subtitles
        if (parentJob.userId !== userId) {
          console.error(
            `User ${userId} is not the owner of job ${parentJobId}. Owner is ${parentJob.userId}`
          );
          return { jobId: "", croppedZipKey: null };
        }

        // Get raw zip key from parent job items (final processed zip)
        const parentRawZipItem = await getJobItemMetaByType(
          parentJobId,
          JobItemType.RAW_ZIP
        );
        if (!parentRawZipItem) {
          console.error("Raw zip path missing for job - job may not be completed", parentJobId);
          return { jobId: "", croppedZipKey: null };
        }

        rawZipKey = parentRawZipItem.storageKey;
        await assertR2ObjectMatchesExpectedSize({
          key: rawZipKey,
          expectedSizeBytes: parentRawZipItem.sizeBytes,
          context: `parent job ${parentJobId} RAW_ZIP`,
        });
        subtitleJobUserId = parentJob.userId;
        parentJobIdForSubtitle = parentJobId;
      } else if (zipKey) {
        // Direct zipKey provided - standalone subtitle removal job
        rawZipKey = zipKey;
        // Best-effort validation (standalone job may not have DB sizeBytes)
        await assertR2ObjectMatchesExpectedSize({
          key: rawZipKey,
          expectedSizeBytes: null,
          context: `standalone zipKey`,
        });
        subtitleJobUserId = userId;
        parentJobIdForSubtitle = null;
      } else {
        console.error("Invalid event data: need parentJobId or zipKey", event.data);
        return { jobId: "", croppedZipKey: null };
      }

      // Always use the provided jobId - check if job already exists
      subtitleJobId = jobId;
      const [existingJob] = await db
        .select()
        .from(ocrJobs)
        .where(eq(ocrJobs.jobId, jobId))
        .limit(1);

      if (existingJob) {
        // Job exists - validate it's a subtitle removal job
        if (existingJob.jobType !== JobType.SUBTITLE_REMOVAL) {
          console.error(
            `Job ${jobId} exists but is not a subtitle removal job. Type: ${existingJob.jobType}`
          );
          return { jobId: "", croppedZipKey: null };
        }

        // Validate ownership
        if (existingJob.userId !== userId) {
          console.error(
            `User ${userId} is not the owner of job ${jobId}. Owner is ${existingJob.userId}`
          );
          return { jobId: "", croppedZipKey: null };
        }

        // Check if cropped zip already exists
        const existingCroppedItem = await db
          .select()
          .from(ocrJobItems)
          .where(
            and(
              eq(ocrJobItems.jobId, subtitleJobId),
              eq(ocrJobItems.itemType, JobItemType.CROPPED_ZIP)
            )
          )
          .limit(1);

        if (existingCroppedItem.length > 0) {
          console.log(`Cropped zip already exists for subtitle job ${subtitleJobId}`);
          return {
            jobId: subtitleJobId,
            croppedZipKey: existingCroppedItem[0].storageKey,
          };
        }
      } else {
        // Job doesn't exist - create it with provided jobId
        await db
          .insert(ocrJobs)
          .values({
            jobId: subtitleJobId,
            userId: subtitleJobUserId,
            jobType: JobType.SUBTITLE_REMOVAL,
            parentJobId: parentJobIdForSubtitle,
            status: JobsStatus.PENDING,
            step: JobStep.PREPROCESSING,
          });

        // Create item for the original zip if standalone
        if (!parentJobIdForSubtitle) {
          await db.insert(ocrJobItems).values({
            jobId: subtitleJobId,
            itemType: JobItemType.ORIGINAL_ZIP,
            storageKey: rawZipKey,
            contentType: "application/zip",
          });
        }
      }

      // Get the subtitle job to use its userId for storage keys
      const [subtitleJob] = await db
        .select()
        .from(ocrJobs)
        .where(eq(ocrJobs.jobId, subtitleJobId))
        .limit(1);

      if (!subtitleJob) {
        console.error("Subtitle job not found after creation", subtitleJobId);
        return { jobId: "", croppedZipKey: null };
      }

      const workspacePaths = buildWorkspacePaths(subtitleJobId);
      const croppedZipKey = getJobCroppedZipKey(subtitleJob.userId, subtitleJobId);
      const croppedThumbnailKey = getJobCroppedThumbnailKey(subtitleJob.userId, subtitleJobId);
      const storageKeys = {
        croppedZipKey,
        croppedThumbnailKey,
      };

      await ensureWorkspaceLayout(workspacePaths);

      // Count images before processing
      const totalImages = await step.run("count-images", () =>
        countProcessableImagesInZip(rawZipKey)
      );

      // Initialize progress tracking
      await persistProgress(subtitleJobId, totalImages, 0);

      // Procesar imágenes y remover subtítulos
      const result = await step.run(
        OcrStepId.PreprocessImagesAndCrops,
        () =>
          streamAndRemoveSubtitles({
            jobId: subtitleJobId,
            zipKey: rawZipKey,
            storageKeys,
            onProgress: async (count: number) => {
              await persistProgress(subtitleJobId, totalImages, count);
            },
          })
      );

      // Final progress update
      await persistProgress(subtitleJobId, totalImages, result.totalImages);

      // Create items in the database (cropped thumbnail linked to cropped zip)
      let croppedZipItemId: string | null = null;

      if (result.croppedZipKey) {
        console.log(
          `Creating CROPPED_ZIP item for job ${subtitleJobId}: ${result.croppedZipKey}, size: ${result.croppedZipSizeBytes ?? 0} bytes, processed images: ${result.totalImages}`
        );
        
        // Always create the item if we have a key, even if size is 0 (shouldn't happen but handle gracefully)
        const [croppedZipItem] = await db
          .insert(ocrJobItems)
          .values({
            jobId: subtitleJobId,
            itemType: JobItemType.CROPPED_ZIP,
            storageKey: result.croppedZipKey,
            sizeBytes: result.croppedZipSizeBytes ?? null,
            contentType: "application/zip",
          })
          .returning({ ocrJobItemId: ocrJobItems.ocrJobItemId });
        
        croppedZipItemId = croppedZipItem.ocrJobItemId;
        console.log(`CROPPED_ZIP item created with ID: ${croppedZipItemId}, storageKey: ${result.croppedZipKey}`);
        
        if (!result.croppedZipSizeBytes || result.croppedZipSizeBytes === 0) {
          console.warn(
            `Warning: CROPPED_ZIP item created but sizeBytes is 0 or null. This might indicate an issue with the upload.`
          );
        }
      } else {
        console.error(
          `Failed to create CROPPED_ZIP item: croppedZipKey is null or undefined. Processed images: ${result.totalImages}`
        );
      }

      if (result.croppedThumbnailKey && croppedZipItemId) {
        console.log(
          `Creating CROPPED_THUMBNAIL item for job ${subtitleJobId}: ${result.croppedThumbnailKey}`
        );
        
        await db.insert(ocrJobItems).values({
          jobId: subtitleJobId,
          itemType: JobItemType.CROPPED_THUMBNAIL,
          storageKey: result.croppedThumbnailKey,
          contentType: "image/jpeg",
          parentItemId: croppedZipItemId,
        });
        
        console.log(`CROPPED_THUMBNAIL item created`);
      } else if (result.croppedThumbnailKey && !croppedZipItemId) {
        console.warn(
          `Thumbnail created but CROPPED_ZIP item ID is missing, cannot link thumbnail`
        );
      }

      // Update subtitle job status to DONE
      await db
        .update(ocrJobs)
        .set({
          status: JobsStatus.DONE,
          step: JobStep.DOCS_BUILT,
        })
        .where(eq(ocrJobs.jobId, subtitleJobId));

      return {
        jobId: subtitleJobId,
        croppedZipKey: result.croppedZipKey,
      };
    } catch (err) {
      const eventData = event.data as {
        jobId?: string;
        parentJobId?: string;
      };
      
      const jobIdToUpdate = eventData.jobId;
      const parentJobIdToSearch = eventData.parentJobId;
      
      console.error(
        "removeSubtitlesFromImages failed",
        jobIdToUpdate || parentJobIdToSearch || "standalone",
        err
      );

      const errorMessage =
        err instanceof Error ? err.message : "Unknown error in subtitle removal job";

      // Try to find and update subtitle job if it exists
      if (jobIdToUpdate) {
        // Try to update by provided jobId
        await db
          .update(ocrJobs)
          .set({
            status: JobsStatus.ERROR,
            error: errorMessage,
          })
          .where(eq(ocrJobs.jobId, jobIdToUpdate));
      } else if (parentJobIdToSearch) {
        // Try to find by parentJobId
        const subtitleJob = await db
          .select()
          .from(ocrJobs)
          .where(
            and(
              eq(ocrJobs.parentJobId, parentJobIdToSearch),
              eq(ocrJobs.jobType, JobType.SUBTITLE_REMOVAL)
            )
          )
          .limit(1)
          .then((rows) => rows[0]);

        if (subtitleJob) {
          await db
            .update(ocrJobs)
            .set({
              status: JobsStatus.ERROR,
              error: errorMessage,
            })
            .where(eq(ocrJobs.jobId, subtitleJob.jobId));
        }
      }

      throw err;
    }
  }
);

