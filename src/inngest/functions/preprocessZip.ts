import { inngest } from "@/inngest/client";
import { db } from "@/db";
import { ocrJobs } from "@/db/schema";
import { eq } from "drizzle-orm";
import {
  validateProcessableImageEntry,
  compareImageFilenames,
  normalizeBufferTo1280x720,
  cropSubtitleFromBuffer,
  createThumbnailFromBuffer,
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
import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { InngestEvents } from "@/types/enums/inngest";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import {
  getJobRawArchiveKey,
  getJobCropKey,
  getJobThumbnailKey,
  uploadBufferToObject,
  uploadStreamToObject,
  createSignedDownloadUrlWithTtl,
  downloadObjectStream,
} from "@/utils/storage";
import * as fs from "node:fs/promises";
import archiver from "archiver";
import { Transform } from "node:stream";
import unzipper from "unzipper";

export const BATCH_SIZE = 20;
const CROP_SIGNED_URL_TTL_SECONDS = 60 * 60 * 24; // 24 hours

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
  rawZipKey: string;
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
  rawZipKey: getJobRawArchiveKey(jobId),
});

const ensureWorkspaceLayout = async (paths: WorkspacePaths) => {
  await Promise.all([
    fs.mkdir(paths.jobRootDir, { recursive: true }),
    fs.mkdir(paths.rawDir, { recursive: true }),
    fs.mkdir(paths.normalizedDir, { recursive: true }),
    fs.mkdir(paths.cropsDir, { recursive: true }),
    fs.mkdir(VOLUME_DIRS.txtBase, { recursive: true }),
    fs.mkdir(VOLUME_DIRS.wordBase, { recursive: true }),
    fs.mkdir(VOLUME_DIRS.tmpBase, { recursive: true }),
  ]);
};

const chunkArray = <T>(array: T[], chunkSize: number): T[][] => {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
};

export const preprocessZip = inngest.createFunction(
  {
    id: InngestFunctions.PREPROCESS_ZIP,
    timeouts: {
      finish: "10h", // Maximum allowed timeout for preprocessing a large ZIP file
    },
  },
  { event: InngestEvents.ZIP_UPLOADED },
  async ({ event, step }) => {
    const { jobId, zipKey, userId } = event.data as {
      jobId: string;
      zipKey: string;
      userId: string;
    };

    if (!userId) {
      console.error("UserId missing in event data", event.data);
      await db
        .update(ocrJobs)
        .set({
          status: JobsStatus.ERROR,
          error: "UserId missing in event data",
        })
        .where(eq(ocrJobs.jobId, jobId));
      return;
    }

    const [job] = await db
      .select()
      .from(ocrJobs)
      .where(eq(ocrJobs.jobId, jobId))
      .limit(1);

    if (!job) {
      console.error("Job not found", jobId);
      return;
    }

    const storageZipKey = zipKey ?? job.zipPath;
    if (!storageZipKey) {
      console.error("Zip key missing for job", jobId);
      return;
    }

    const workspacePaths = buildWorkspacePaths(jobId);
    const storageKeys = buildStorageKeys(jobId);

    await step.run("ensure-workspace", () =>
      ensureWorkspaceLayout(workspacePaths)
    );

    const streamingResult = await step.run(
      "preprocess-images-and-crops",
      async () => {
        const zipReadable = await downloadObjectStream(storageZipKey);
        const unzipStream = zipReadable.pipe(
          unzipper.Parse({ forceStream: true })
        );

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

          if (processable.shouldIncludeInZip) {
            const originalExt =
              processable.originalName.match(/\.(png|jpe?g)$/i)?.[0] || ".png";
            const zipFilename = `${processable.baseName}${originalExt}`;
            archive.append(fileBuffer, { name: zipFilename });
          }

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
            const thumbnailBuffer =
              await createThumbnailFromBuffer(normalizedBuffer);
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
      }
    );

    await step.run("update-job-preprocessing", async () => {
      await db.update(ocrJobs).set({
        rawZipPath: streamingResult.rawZipKey,
        rawZipSizeBytes: streamingResult.rawZipSizeBytes,
        thumbnailKey: streamingResult.thumbnailKey,
        step: JobStep.BATCH_SUBMITTED,
        totalImages: streamingResult.totalImages,
        processedImages: streamingResult.totalImages,
        status: JobsStatus.PROCESSING,
      });
    });

    if (!streamingResult.cropsMeta.length) {
      throw new Error("No crops were generated from the provided ZIP file.");
    }

    // Divide crops into batches and dispatch events for each batch
    const cropsChunks = chunkArray(streamingResult.cropsMeta, BATCH_SIZE);
    const totalBatches = cropsChunks.length;

    console.log(
      `Processing ${streamingResult.totalImages} images in ${totalBatches} batches for job ${jobId}`
    );

    // Dispatch events to create each batch
    await step.run("dispatch-batch-events", async () => {
      const events = cropsChunks.map((chunk, batchIndex) => ({
        name: InngestEvents.BATCH_CREATE,
        data: {
          jobId,
          userId,
          batchIndex,
          totalBatches,
          cropsMeta: chunk,
          globalStartIndex: batchIndex * BATCH_SIZE,
        },
      }));

      await inngest.send(events);
    });

    return {
      jobId,
      totalImages: streamingResult.totalImages,
      totalBatches,
    };
  }
);

