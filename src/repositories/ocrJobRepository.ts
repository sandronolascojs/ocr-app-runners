import { db } from "@/db";
import {
  ocrJobFrames,
  ocrJobItems,
  ocrJobs,
} from "@/db/schema";
import { and, eq, sql } from "drizzle-orm";
import { JobItemType } from "@/types/enums/jobs/jobItemType.enum";

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
