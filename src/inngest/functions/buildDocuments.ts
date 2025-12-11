import * as fs from "node:fs/promises";
import fsSync from "node:fs";
import { inngest } from "@/inngest/client";
import { db } from "@/db";
import { ocrJobs, ocrJobFrames } from "@/db/schema";
import { eq } from "drizzle-orm";
import { buildParagraphsFromFrames } from "@/utils/ocr/paragraphs";
import { writeDocxFromParagraphs } from "@/utils/ocr/docx";
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
} from "@/utils/paths";
import {
  getJobDocxKey,
  getJobRawArchiveKey,
  getJobTxtKey,
  uploadFileToObject,
} from "@/utils/storage";
import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { InngestEvents } from "@/types/enums/inngest";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { BATCH_SIZE } from "./preprocessZip";

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

export const buildDocuments = inngest.createFunction(
  {
    id: InngestFunctions.BUILD_DOCUMENTS,
    timeouts: {
      finish: "30m",
    },
  },
  { event: InngestEvents.RESULTS_SAVED },
  async ({ event, step }) => {
    const { jobId, totalImages } = event.data as {
      jobId: string;
      userId: string;
      totalImages: number;
    };

    const [job] = await db
      .select()
      .from(ocrJobs)
      .where(eq(ocrJobs.jobId, jobId))
      .limit(1);

    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }

    const workspacePaths = buildWorkspacePaths(jobId);
    const storageKeys = buildStorageKeys(jobId);

    const rawZipKeyForJob = await step.run("build-documents", async () => {
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

      await fs.writeFile(workspacePaths.txtPath, txtContent, "utf8");
      await writeDocxFromParagraphs(paragraphs, workspacePaths.docxPath);

      // Calculate file sizes before uploading
      const txtStats = fsSync.statSync(workspacePaths.txtPath);
      const docxStats = fsSync.statSync(workspacePaths.docxPath);

      await uploadFileToObject({
        key: storageKeys.txtKey,
        filePath: workspacePaths.txtPath,
        contentType: "text/plain; charset=utf-8",
      });

      await uploadFileToObject({
        key: storageKeys.docxKey,
        filePath: workspacePaths.docxPath,
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
          step: JobStep.DOCS_BUILT,
        })
        .where(eq(ocrJobs.jobId, jobId));

      // Cleanup
      const dirsToRemove = [
        workspacePaths.rawDir,
        workspacePaths.normalizedDir,
        workspacePaths.cropsDir,
      ];
      const filesToRemove = [
        workspacePaths.zipPath,
        workspacePaths.txtPath,
        workspacePaths.docxPath,
        workspacePaths.rawArchivePath,
      ];

      // Remove all batch JSONL files
      try {
        await fs.unlink(workspacePaths.batchJsonlPath);
      } catch {
        // ignore
      }

      const numberOfBatches = Math.ceil(totalImages / BATCH_SIZE);
      const maxBatchesToCheck = numberOfBatches + 10;

      for (let i = 0; i < maxBatchesToCheck; i++) {
        try {
          const batchPath = getJobBatchJsonlPath(jobId, i);
          await fs.unlink(batchPath);
        } catch {
          // ignore
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

      return storageKeys.rawZipKey;
    });

    return {
      jobId,
      txtKey: storageKeys.txtKey,
      docxKey: storageKeys.docxKey,
      rawZipKey: rawZipKeyForJob,
    };
  }
);

