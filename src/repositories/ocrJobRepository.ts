import { db } from "@/db";
import {
  ocrJobBatches,
  ocrJobCrops,
  ocrJobFrames,
  ocrJobItems,
  ocrJobs,
} from "@/db/schema";
import { and, desc, eq, inArray, sql } from "drizzle-orm";
import { JobItemType } from "@/types/enums/jobs/jobItemType.enum";
import type { OcrBatchStatus } from "@/types/enums/jobs/ocrBatchStatus.enum";
import { OcrCropStatus } from "@/types/enums/jobs/ocrCropStatus.enum";

export type JobItemMeta = {
  storageKey: string;
  sizeBytes: number | null;
};

export type PersistableFrame = {
  jobId: string;
  filename: string;
  baseKey: string;
  index: number;
  text: string;
};

export type OcrJobCropRow = {
  ocrJobCropId: string;
  jobId: string;
  index: number;
  filename: string;
  baseKey: string;
  cropKey: string;
};

export type InFlightBatch = {
  ocrJobBatchId: string;
  batchNo: number;
  startIndex: number;
  endIndexExclusive: number;
  openaiBatchId: string | null;
  status: OcrBatchStatus;
};

export type LastBatchSummary = {
  batchNo: number;
  batchSize: number;
  tokenLimitDetected: boolean;
};

export const getJobById = async (jobId: string) => {
  const [job] = await db
    .select()
    .from(ocrJobs)
    .where(eq(ocrJobs.jobId, jobId))
    .limit(1);
  return job ?? null;
};

export const updateJob = async (
  jobId: string,
  data: Partial<typeof ocrJobs.$inferInsert>
) => {
  await db.update(ocrJobs).set(data).where(eq(ocrJobs.jobId, jobId));
};

export const persistProgress = async (
  jobId: string,
  progress: {
    totalImages: number;
    processedImages: number;
    submittedImages: number;
    totalBatches: number;
    batchesCompleted: number;
  },
  extra?: Record<string, unknown>
) => {
  const allowedFields = ["step", "status", "error"];
  const filteredExtra = extra
    ? Object.fromEntries(
        Object.entries(extra).filter(([key]) => allowedFields.includes(key))
      )
    : {};

  await db
    .update(ocrJobs)
    .set({
      processedImages: progress.processedImages,
      totalImages: progress.totalImages,
      totalBatches: progress.totalBatches,
      batchesCompleted: progress.batchesCompleted,
      submittedImages: progress.submittedImages,
      ...filteredExtra,
    })
    .where(eq(ocrJobs.jobId, jobId));
};

export const getJobItemMetaByType = async (
  jobId: string,
  itemType: JobItemType
): Promise<JobItemMeta | null> => {
  const [item] = await db
    .select({
      storageKey: ocrJobItems.storageKey,
      sizeBytes: ocrJobItems.sizeBytes,
    })
    .from(ocrJobItems)
    .where(and(eq(ocrJobItems.jobId, jobId), eq(ocrJobItems.itemType, itemType)))
    .limit(1);

  if (!item?.storageKey) return null;
  return { storageKey: item.storageKey, sizeBytes: item.sizeBytes ?? null };
};

export const getJobItemByType = async (
  jobId: string,
  itemType: JobItemType
): Promise<string | null> => {
  const [item] = await db
    .select({ storageKey: ocrJobItems.storageKey })
    .from(ocrJobItems)
    .where(and(eq(ocrJobItems.jobId, jobId), eq(ocrJobItems.itemType, itemType)))
    .limit(1);

  return item?.storageKey ?? null;
};

export const upsertJobItem = async ({
  jobId,
  itemType,
  storageKey,
  sizeBytes,
  contentType,
  parentItemId,
}: {
  jobId: string;
  itemType: JobItemType;
  storageKey: string;
  sizeBytes?: number;
  contentType?: string;
  parentItemId?: string;
}): Promise<string> => {
  const [existing] = await db
    .select({ ocrJobItemId: ocrJobItems.ocrJobItemId })
    .from(ocrJobItems)
    .where(and(eq(ocrJobItems.jobId, jobId), eq(ocrJobItems.itemType, itemType)))
    .limit(1);

  if (existing?.ocrJobItemId) {
    await db
      .update(ocrJobItems)
      .set({
        storageKey,
        sizeBytes: sizeBytes ?? null,
        contentType: contentType ?? null,
        parentItemId: parentItemId ?? null,
      })
      .where(eq(ocrJobItems.ocrJobItemId, existing.ocrJobItemId));
    return existing.ocrJobItemId;
  }

  const [newItem] = await db
    .insert(ocrJobItems)
    .values({
      jobId,
      itemType,
      storageKey,
      sizeBytes: sizeBytes ?? null,
      contentType: contentType ?? null,
      parentItemId: parentItemId ?? null,
    })
    .returning({ ocrJobItemId: ocrJobItems.ocrJobItemId });
  return newItem.ocrJobItemId;
};

export const updateJobItemSizeByType = async (
  jobId: string,
  itemType: JobItemType,
  sizeBytes: number
) => {
  await db
    .update(ocrJobItems)
    .set({ sizeBytes })
    .where(and(eq(ocrJobItems.jobId, jobId), eq(ocrJobItems.itemType, itemType)));
};

export const deleteJobItemByType = async (
  jobId: string,
  itemType: JobItemType
) => {
  await db
    .delete(ocrJobItems)
    .where(and(eq(ocrJobItems.jobId, jobId), eq(ocrJobItems.itemType, itemType)));
};

export const countCropsForJob = async (jobId: string): Promise<number> => {
  const [{ count }] = await db
    .select({ count: sql<number>`count(*)` })
    .from(ocrJobCrops)
    .where(eq(ocrJobCrops.jobId, jobId));
  return count ?? 0;
};

export const replaceCropsForJob = async (
  jobId: string,
  crops: Array<{
    index: number;
    filename: string;
    baseKey: string;
    cropKey: string;
    status: OcrCropStatus;
  }>
) => {
  await db.transaction(async (tx) => {
    await tx.delete(ocrJobCrops).where(eq(ocrJobCrops.jobId, jobId));
    if (crops.length) {
      await tx.insert(ocrJobCrops).values(
        crops.map((crop) => ({
          jobId,
          index: crop.index,
          filename: crop.filename,
          baseKey: crop.baseKey,
          cropKey: crop.cropKey,
          status: crop.status,
        }))
      );
    }
  });
};

export const getCropsForJob = async (jobId: string): Promise<OcrJobCropRow[]> =>
  db
    .select({
      ocrJobCropId: ocrJobCrops.ocrJobCropId,
      jobId: ocrJobCrops.jobId,
      index: ocrJobCrops.index,
      filename: ocrJobCrops.filename,
      baseKey: ocrJobCrops.baseKey,
      cropKey: ocrJobCrops.cropKey,
    })
    .from(ocrJobCrops)
    .where(eq(ocrJobCrops.jobId, jobId))
    .orderBy(ocrJobCrops.index);

export const getCropsForBatchRange = async (params: {
  jobId: string;
  startIndex: number;
  endIndexExclusive: number;
}): Promise<OcrJobCropRow[]> =>
  db
    .select({
      ocrJobCropId: ocrJobCrops.ocrJobCropId,
      jobId: ocrJobCrops.jobId,
      index: ocrJobCrops.index,
      filename: ocrJobCrops.filename,
      baseKey: ocrJobCrops.baseKey,
      cropKey: ocrJobCrops.cropKey,
    })
    .from(ocrJobCrops)
    .where(
      and(
        eq(ocrJobCrops.jobId, params.jobId),
        sql`${ocrJobCrops.index} >= ${params.startIndex}`,
        sql`${ocrJobCrops.index} < ${params.endIndexExclusive}`
      )
    )
    .orderBy(ocrJobCrops.index);

export const getCropsByIds = async (cropIds: string[]): Promise<OcrJobCropRow[]> => {
  if (!cropIds.length) return [];
  return db
    .select({
      ocrJobCropId: ocrJobCrops.ocrJobCropId,
      jobId: ocrJobCrops.jobId,
      index: ocrJobCrops.index,
      filename: ocrJobCrops.filename,
      baseKey: ocrJobCrops.baseKey,
      cropKey: ocrJobCrops.cropKey,
    })
    .from(ocrJobCrops)
    .where(inArray(ocrJobCrops.ocrJobCropId, cropIds))
    .orderBy(ocrJobCrops.index);
};

export const getPendingCrops = async (params: {
  jobId: string;
  statuses: OcrCropStatus[];
  limit: number;
}): Promise<OcrJobCropRow[]> =>
  db
    .select({
      ocrJobCropId: ocrJobCrops.ocrJobCropId,
      jobId: ocrJobCrops.jobId,
      index: ocrJobCrops.index,
      filename: ocrJobCrops.filename,
      baseKey: ocrJobCrops.baseKey,
      cropKey: ocrJobCrops.cropKey,
    })
    .from(ocrJobCrops)
    .where(
      and(
        eq(ocrJobCrops.jobId, params.jobId),
        inArray(ocrJobCrops.status, params.statuses)
      )
    )
    .orderBy(ocrJobCrops.index)
    .limit(params.limit);

export const updateCropsStatus = async (params: {
  cropIds: string[];
  status: OcrCropStatus;
  lastError?: string | null;
}) => {
  if (!params.cropIds.length) return;
  await db
    .update(ocrJobCrops)
    .set({ status: params.status, lastError: params.lastError ?? null })
    .where(inArray(ocrJobCrops.ocrJobCropId, params.cropIds));
};

export const updateCropsFailedRetryable = async (
  failures: Array<{ cropId: string; message: string }>
) => {
  const uniqueFailures = new Map<string, string>();
  for (const failure of failures) {
    if (!uniqueFailures.has(failure.cropId)) {
      uniqueFailures.set(failure.cropId, failure.message);
    }
  }
  const cropIds = Array.from(uniqueFailures.keys());
  if (!cropIds.length) return;

  const errorCase = sql`case ${ocrJobCrops.ocrJobCropId} ${sql.join(
    cropIds.map((id) => sql`when ${id} then ${uniqueFailures.get(id)}`),
    sql` `
  )} else ${ocrJobCrops.lastError} end`;

  await db
    .update(ocrJobCrops)
    .set({ status: OcrCropStatus.FAILED_RETRYABLE, lastError: errorCase })
    .where(inArray(ocrJobCrops.ocrJobCropId, cropIds));
};

export const upsertFrames = async (frames: PersistableFrame[]): Promise<void> => {
  if (!frames.length) return;
  await db
    .insert(ocrJobFrames)
    .values(frames)
    .onConflictDoUpdate({
      target: [ocrJobFrames.jobId, ocrJobFrames.index],
      set: {
        filename: sql`excluded.filename`,
        baseKey: sql`excluded.base_key`,
        text: sql`excluded.text`,
        updatedAt: new Date(),
      },
    });
};

export const getFramesForJob = async (jobId: string) =>
  db.select().from(ocrJobFrames).where(eq(ocrJobFrames.jobId, jobId));

export const getNextBatchNo = async (jobId: string): Promise<number> => {
  const [{ max }] = await db
    .select({ max: sql<number>`coalesce(max(${ocrJobBatches.batchNo}), 0)` })
    .from(ocrJobBatches)
    .where(eq(ocrJobBatches.jobId, jobId));
  return (max ?? 0) + 1;
};

export const getLastBatchSummary = async (
  jobId: string
): Promise<LastBatchSummary | null> => {
  const [lastBatch] = await db
    .select({
      batchNo: ocrJobBatches.batchNo,
      batchSize: ocrJobBatches.batchSize,
      tokenLimitDetected: ocrJobBatches.tokenLimitDetected,
    })
    .from(ocrJobBatches)
    .where(eq(ocrJobBatches.jobId, jobId))
    .orderBy(desc(ocrJobBatches.batchNo))
    .limit(1);
  return lastBatch ?? null;
};

export const getInFlightBatch = async (
  jobId: string,
  statuses: OcrBatchStatus[]
): Promise<InFlightBatch | null> => {
  const [inFlight] = await db
    .select({
      ocrJobBatchId: ocrJobBatches.ocrJobBatchId,
      batchNo: ocrJobBatches.batchNo,
      startIndex: ocrJobBatches.startIndex,
      endIndexExclusive: ocrJobBatches.endIndexExclusive,
      openaiBatchId: ocrJobBatches.openaiBatchId,
      status: ocrJobBatches.status,
    })
    .from(ocrJobBatches)
    .where(
      and(eq(ocrJobBatches.jobId, jobId), inArray(ocrJobBatches.status, statuses))
    )
    .orderBy(desc(ocrJobBatches.batchNo))
    .limit(1);
  return inFlight ?? null;
};

export const insertBatch = async (
  data: Omit<typeof ocrJobBatches.$inferInsert, "ocrJobBatchId">
): Promise<string> => {
  const [batchRow] = await db
    .insert(ocrJobBatches)
    .values([data])
    .returning({ ocrJobBatchId: ocrJobBatches.ocrJobBatchId });
  return batchRow.ocrJobBatchId;
};

export const updateBatch = async (
  ocrJobBatchId: string,
  data: Partial<typeof ocrJobBatches.$inferInsert>
) => {
  await db.update(ocrJobBatches).set(data).where(eq(ocrJobBatches.ocrJobBatchId, ocrJobBatchId));
};
