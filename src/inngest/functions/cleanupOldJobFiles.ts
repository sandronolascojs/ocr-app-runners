import { inngest } from "@/inngest/client";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { db } from "@/db";
import { ocrJobs, ocrJobItems } from "@/db/schema";
import { lte, eq } from "drizzle-orm";
import { deleteObjectsByPrefix } from "@/utils/storage/r2";
import { getJobRootKey } from "@/utils/storage/keys";

const RETENTION_DAYS = 30;

export const cleanupOldJobFiles = inngest.createFunction(
  {
    id: InngestFunctions.CLEANUP_OLD_JOB_FILES,
    concurrency: { limit: 1 },
  },
  { cron: "0 3 * * *" }, // every day at 3 AM
  async ({ step, logger }) => {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - RETENTION_DAYS);

    const oldJobs = await step.run("fetch-old-jobs", async () => {
      const jobs = await db
        .select({
          jobId: ocrJobs.jobId,
          userId: ocrJobs.userId,
        })
        .from(ocrJobs)
        .where(lte(ocrJobs.createdAt, cutoff));

      return jobs;
    });

    if (oldJobs.length === 0) {
      logger.info("No jobs older than 30 days found. Nothing to clean up.");
      return { deleted: 0, skipped: 0 };
    }

    logger.info(`Found ${oldJobs.length} jobs older than ${RETENTION_DAYS} days`);

    let deleted = 0;
    let skipped = 0;

    for (const job of oldJobs) {
      const result = await step.run(`cleanup-${job.jobId}`, async () => {
        const prefix = getJobRootKey(job.userId, job.jobId);

        try {
          const deletedCount = await deleteObjectsByPrefix(prefix);

          // Clear storage keys from job items so we know files are gone
          await db
            .delete(ocrJobItems)
            .where(eq(ocrJobItems.jobId, job.jobId));

          return { success: true, deletedCount };
        } catch (error) {
          logger.error(
            `Failed to cleanup files for job ${job.jobId}: ${error instanceof Error ? error.message : String(error)}`
          );
          return { success: false, deletedCount: 0 };
        }
      });

      if (result.success) {
        deleted++;
      } else {
        skipped++;
      }
    }

    logger.info(`Cleanup complete: ${deleted} jobs cleaned, ${skipped} failed`);
    return { deleted, skipped };
  }
);
