#!/usr/bin/env tsx

/**
 * Cleanup script that deletes R2 objects for non-completed jobs while keeping
 * every object that belongs to jobs marked as DONE. It also removes failed
 * jobs from the database to keep records consistent.
 *
 * Usage:
 *   pnpm tsx scripts/cleanup-r2-failed-jobs.ts
 *
 * Run without confirmation (CI/CD):
 *   pnpm tsx scripts/cleanup-r2-failed-jobs.ts --force
 */

import {
  AbortMultipartUploadCommand,
  DeleteObjectsCommand,
  ListMultipartUploadsCommand,
  ListObjectsV2Command,
  type DeleteObjectsCommandOutput,
  type ListObjectsV2CommandOutput,
  S3Client,
} from "@aws-sdk/client-s3";
import { inArray } from "drizzle-orm";

import { env } from "../src/config/env.config";
import { db } from "../src/db";
import { ocrJobs } from "../src/db/schema/jobs";
import { getJobRootKey } from "../src/utils/storage/keys";
import { JobsStatus } from "../src/types";

const r2Endpoint =
  env.CLOUDFLARE_R2_S3_ENDPOINT ??
  `https://${env.CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;

// Remove bucket name from endpoint when forcePathStyle is used.
const cleanEndpoint = r2Endpoint.includes(`/${env.CLOUDFLARE_R2_BUCKET_NAME}`)
  ? r2Endpoint.replace(`/${env.CLOUDFLARE_R2_BUCKET_NAME}`, "").replace(/\/$/, "")
  : r2Endpoint.replace(/\/$/, "");

const r2Client = new S3Client({
  region: "auto",
  endpoint: cleanEndpoint,
  forcePathStyle: true,
  credentials: {
    accessKeyId: env.CLOUDFLARE_R2_ACCESS_KEY_ID,
    secretAccessKey: env.CLOUDFLARE_R2_SECRET_ACCESS_KEY,
  },
});

const BUCKET_NAME = env.CLOUDFLARE_R2_BUCKET_NAME;
const DELETE_BATCH_SIZE = 1000; // S3/R2 limit
const PROTECTED_STATUSES: JobsStatus[] = [JobsStatus.DONE];
const FAILED_STATUSES: JobsStatus[] = [JobsStatus.ERROR];

interface ObjectInfo {
  key: string;
  size: number;
}

interface JobPrefix {
  jobId: string;
  userId: string;
  prefix: string;
  status: JobsStatus;
}

const isKeyNotFoundError = (error: unknown): boolean => {
  if (typeof error !== "object" || error === null) {
    return false;
  }

  const errorObj = error as Record<string, unknown>;
  const message = String(errorObj.message ?? "").toLowerCase();
  const code = String(errorObj.Code ?? errorObj.code ?? "").toLowerCase();
  const name = String(errorObj.name ?? "").toLowerCase();

  return (
    message.includes("the specified key does not exist") ||
    message.includes("nosuchkey") ||
    message.includes("not found") ||
    code === "nosuchkey" ||
    name === "nosuchkey" ||
    name === "notfound"
  );
};

const formatBytes = (bytes: number): string => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${Math.round((bytes / Math.pow(k, i)) * 100) / 100} ${sizes[i]}`;
};

const fetchJobPrefixesByStatus = async (statuses: JobsStatus[]): Promise<JobPrefix[]> => {
  if (statuses.length === 0) return [];

  const rows = await db
    .select({
      jobId: ocrJobs.jobId,
      userId: ocrJobs.userId,
      status: ocrJobs.status,
    })
    .from(ocrJobs)
    .where(inArray(ocrJobs.status, statuses));

  return rows.map((row) => ({
    ...row,
    prefix: getJobRootKey(row.userId, row.jobId),
  }));
};

const listAllObjects = async (): Promise<ObjectInfo[]> => {
  const objects: ObjectInfo[] = [];
  let continuationToken: string | undefined;

  console.log("Listing all objects in the bucket...");
  console.log(`  Bucket: ${BUCKET_NAME}`);
  console.log(`  Endpoint: ${cleanEndpoint}`);

  try {
    do {
      const command = new ListObjectsV2Command({
        Bucket: BUCKET_NAME,
        ContinuationToken: continuationToken,
        MaxKeys: 1000,
      });

      const response: ListObjectsV2CommandOutput = await r2Client.send(command);

      if (response.Contents) {
        for (const object of response.Contents) {
          if (object.Key && object.Size !== undefined) {
            objects.push({
              key: object.Key,
              size: object.Size,
            });
          }
        }
      }

      continuationToken = response.NextContinuationToken;
      if (objects.length > 0 && (objects.length % 1000 === 0 || continuationToken)) {
        console.log(`  Found ${objects.length} objects so far...`);
      }
    } while (continuationToken);

    console.log(`  Completed listing: ${objects.length} objects found`);
  } catch (error) {
    console.error("  Error while listing objects:", error);
    if (isKeyNotFoundError(error)) {
      console.log("  Bucket appears empty.");
      return [];
    }
    throw error;
  }

  return objects;
};

const filterDeletableObjects = (
  objects: ObjectInfo[],
  protectedPrefixes: JobPrefix[]
): { deletable: ObjectInfo[]; skipped: ObjectInfo[] } => {
  if (protectedPrefixes.length === 0) {
    return { deletable: objects, skipped: [] };
  }

  const protectedSet = protectedPrefixes.map((item) => `${item.prefix}/`);

  const deletable: ObjectInfo[] = [];
  const skipped: ObjectInfo[] = [];

  for (const object of objects) {
    const isProtected = protectedSet.some((prefix) => object.key.startsWith(prefix));
    if (isProtected) {
      skipped.push(object);
    } else {
      deletable.push(object);
    }
  }

  return { deletable, skipped };
};

const deleteObjectsBatch = async (
  objects: ObjectInfo[],
  batchIndex: number,
  totalBatches: number
): Promise<number> => {
  try {
    const command = new DeleteObjectsCommand({
      Bucket: BUCKET_NAME,
      Delete: {
        Objects: objects.map((obj) => ({ Key: obj.key })),
      },
    });

    const response: DeleteObjectsCommandOutput = await r2Client.send(command);

    if (response.Errors && response.Errors.length > 0) {
      const realErrors = response.Errors.filter((error) => error.Code !== "NoSuchKey");
      if (realErrors.length > 0) {
        const errorDetails = realErrors
          .map(
            (error) =>
              `${error.Key ?? "unknown"}: ${error.Code ?? "unknown"} - ${error.Message ?? "no message"}`
          )
          .join("; ");

        console.warn(
          `  Warning: Batch ${batchIndex + 1}/${totalBatches} had failures: ${errorDetails}`
        );
      }

      const notFoundCount = response.Errors.filter((error) => error.Code === "NoSuchKey").length;
      if (notFoundCount > 0) {
        console.log(
          `  Info: Batch ${batchIndex + 1}/${totalBatches} - ${notFoundCount} object(s) already missing`
        );
      }
    }

    const deletedCount = response.Deleted?.length ?? 0;
    const notFoundCount =
      response.Errors?.filter((error) => error.Code === "NoSuchKey").length ?? 0;
    const totalProcessed = deletedCount + notFoundCount;

    if (totalProcessed > 0) {
      console.log(
        `  Batch ${batchIndex + 1}/${totalBatches}: processed ${totalProcessed} (${deletedCount} deleted, ${notFoundCount} already missing)`
      );
    }

    return deletedCount;
  } catch (error) {
    if (isKeyNotFoundError(error)) {
      console.log(`  Info: Batch ${batchIndex + 1}/${totalBatches} objects already removed`);
      return objects.length;
    }

    console.warn(
      `  Warning: Batch ${batchIndex + 1}/${totalBatches} unexpected error: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
    throw error;
  }
};

const deleteAllObjects = async (objects: ObjectInfo[]): Promise<number> => {
  if (objects.length === 0) {
    console.log("No objects to delete.");
    return 0;
  }

  const totalBatches = Math.ceil(objects.length / DELETE_BATCH_SIZE);
  let totalDeleted = 0;
  let skippedBatches = 0;

  console.log(`Deleting ${objects.length} objects in ${totalBatches} batch(es)...`);

  for (let i = 0; i < objects.length; i += DELETE_BATCH_SIZE) {
    const batch = objects.slice(i, i + DELETE_BATCH_SIZE);
    const batchIndex = Math.floor(i / DELETE_BATCH_SIZE);

    try {
      const deletedCount = await deleteObjectsBatch(batch, batchIndex, totalBatches);
      totalDeleted += deletedCount;
    } catch (error) {
      if (isKeyNotFoundError(error)) {
        console.log(
          `  Info: Batch ${batchIndex + 1}/${totalBatches} objects already removed, continuing...`
        );
        totalDeleted += batch.length;
        skippedBatches++;
        continue;
      }

      console.warn(
        `  Warning: Batch ${batchIndex + 1}/${totalBatches} - ${
          error instanceof Error ? error.message : String(error)
        }. Continuing...`
      );
      skippedBatches++;
    }
  }

  if (skippedBatches > 0) {
    console.log(`  Info: ${skippedBatches} batch(es) had issues but cleanup continued.`);
  }

  return totalDeleted;
};

const listAllMultipartUploads = async () => {
  let keyMarker: string | undefined;
  let uploadIdMarker: string | undefined;
  let uploads = 0;

  console.log("Listing all in-progress multipart uploads...");

  try {
    do {
      const response = await r2Client.send(
        new ListMultipartUploadsCommand({
          Bucket: BUCKET_NAME,
          KeyMarker: keyMarker,
          UploadIdMarker: uploadIdMarker,
        })
      );

      uploads += response.Uploads?.length ?? 0;
      keyMarker = response.NextKeyMarker;
      uploadIdMarker = response.NextUploadIdMarker;
    } while (keyMarker || uploadIdMarker);
  } catch (error) {
    if (isKeyNotFoundError(error)) {
      console.log("  No multipart uploads found.");
      return 0;
    }
    console.warn(
      `  Warning: failed to list multipart uploads: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
    return uploads;
  }

  return uploads;
};

const abortAllMultipartUploads = async (): Promise<number> => {
  const uploads: { key: string; uploadId: string }[] = [];
  let keyMarker: string | undefined;
  let uploadIdMarker: string | undefined;

  console.log("Collecting multipart uploads to abort...");

  do {
    const response = await r2Client.send(
      new ListMultipartUploadsCommand({
        Bucket: BUCKET_NAME,
        KeyMarker: keyMarker,
        UploadIdMarker: uploadIdMarker,
      })
    );

    if (response.Uploads) {
      for (const upload of response.Uploads) {
        if (upload.Key && upload.UploadId) {
          uploads.push({ key: upload.Key, uploadId: upload.UploadId });
        }
      }
    }

    keyMarker = response.NextKeyMarker;
    uploadIdMarker = response.NextUploadIdMarker;
  } while (keyMarker || uploadIdMarker);

  if (uploads.length === 0) {
    console.log("No multipart uploads to abort.");
    return 0;
  }

  console.log(`Aborting ${uploads.length} multipart upload(s)...`);

  let abortedCount = 0;
  let failedCount = 0;

  for (let i = 0; i < uploads.length; i++) {
    const upload = uploads[i];

    try {
      await r2Client.send(
        new AbortMultipartUploadCommand({
          Bucket: BUCKET_NAME,
          Key: upload.key,
          UploadId: upload.uploadId,
        })
      );
      abortedCount++;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      if (
        isKeyNotFoundError(error) ||
        errorMessage.includes("NoSuchUpload") ||
        errorMessage.includes("not found") ||
        errorMessage.includes("does not exist")
      ) {
        abortedCount++;
      } else {
        failedCount++;
        console.warn(
          `  Warning: failed to abort ${upload.key} (${upload.uploadId}): ${errorMessage}`
        );
      }
    }

    if ((i + 1) % 50 === 0 || i === uploads.length - 1) {
      console.log(
        `  Progress: ${i + 1}/${uploads.length} processed (${abortedCount} aborted, ${failedCount} failed)`
      );
    }
  }

  console.log(
    `Aborted ${abortedCount} multipart upload(s)${failedCount > 0 ? `, ${failedCount} failed` : ""}`
  );

  return abortedCount;
};

const deleteFailedJobsFromDb = async (failedJobs: JobPrefix[]): Promise<number> => {
  if (failedJobs.length === 0) {
    console.log("No failed jobs to remove from the database.");
    return 0;
  }

  const jobIds = failedJobs.map((job) => job.jobId);

  const deleted = await db.delete(ocrJobs).where(inArray(ocrJobs.jobId, jobIds));

  // Drizzle returns the number of affected rows in the `rowCount` property on Postgres.
  const rowCount = "rowCount" in deleted && typeof deleted.rowCount === "number"
    ? deleted.rowCount
    : jobIds.length;

  console.log(`Removed ${rowCount} failed job record(s) from the database.`);

  return rowCount;
};

const summarize = (objects: ObjectInfo[]): { totalSize: number; count: number } => {
  const totalSize = objects.reduce((sum, obj) => sum + obj.size, 0);
  return { totalSize, count: objects.length };
};

const main = async () => {
  const force = process.argv.includes("--force");

  console.log("=".repeat(60));
  console.log("R2 Cleanup - Skip completed jobs and purge failed jobs");
  console.log("=".repeat(60));
  console.log(`Bucket: ${BUCKET_NAME}`);
  console.log(`Endpoint original: ${r2Endpoint}`);
  console.log(`Endpoint used: ${cleanEndpoint}`);
  console.log(`Force Path Style: true`);
  console.log(`Access Key ID: ${env.CLOUDFLARE_R2_ACCESS_KEY_ID.substring(0, 8)}...`);
  console.log("=".repeat(60));

  if (!force) {
    console.log("\nWARNING: This script will delete objects for non-completed jobs and remove failed jobs from the database.");
    console.log("Press Ctrl+C to cancel.\n");
    console.log("To proceed without waiting, use: --force\n");
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }

  const [protectedPrefixes, failedJobPrefixes] = await Promise.all([
    fetchJobPrefixesByStatus(PROTECTED_STATUSES),
    fetchJobPrefixesByStatus(FAILED_STATUSES),
  ]);

  console.log(`Protected job prefixes (DONE): ${protectedPrefixes.length}`);
  console.log(`Failed jobs to purge from DB: ${failedJobPrefixes.length}`);

  const multipartCount = await listAllMultipartUploads();
  if (multipartCount > 0) {
    await abortAllMultipartUploads();
  }

  const objects = await listAllObjects();
  const { deletable, skipped } = filterDeletableObjects(objects, protectedPrefixes);
  const deletableSummary = summarize(deletable);
  const skippedSummary = summarize(skipped);

  console.log("\nSummary:");
  console.log(`  Objects total: ${objects.length}`);
  console.log(`  Objects protected (DONE jobs): ${skippedSummary.count} (${formatBytes(skippedSummary.totalSize)})`);
  console.log(`  Objects to delete: ${deletableSummary.count} (${formatBytes(deletableSummary.totalSize)})`);
  if (failedJobPrefixes.length > 0) {
    console.log(`  Failed jobs scheduled for DB deletion: ${failedJobPrefixes.length}`);
  }
  console.log("=".repeat(60));

  if (!force) {
    console.log("\nProceeding in 10 seconds... Press Ctrl+C to abort.\n");
    await new Promise((resolve) => setTimeout(resolve, 10_000));
  }

  const deletedCount = await deleteAllObjects(deletable);
  await deleteFailedJobsFromDb(failedJobPrefixes);

  console.log("\n".repeat(1) + "=".repeat(60));
  console.log("Cleanup complete:");
  console.log(`  Objects deleted: ${deletedCount}`);
  console.log(`  Objects kept (completed jobs): ${skippedSummary.count}`);
  console.log(`  Failed DB jobs removed: ${failedJobPrefixes.length}`);
  console.log("=".repeat(60));
};

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});

