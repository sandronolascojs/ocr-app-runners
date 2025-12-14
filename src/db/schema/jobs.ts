import { bigint, integer, pgTable, text } from "drizzle-orm/pg-core";
import { createdAt, createIdField, jobStatusEnum, jobStepEnum, jobItemTypeEnum, jobTypeEnum, updatedAt } from "./utils";
import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { JobType } from "@/types/enums/jobs/jobType.enum";

export const ocrJobs = pgTable("ocr_jobs", {
  ocrJobId: createIdField({ name: "ocr_job_id" }),

  jobId: text("job_id").notNull().unique(),

  userId: text("user_id").notNull(),

  jobType: jobTypeEnum("job_type").notNull().default(JobType.OCR),

  parentJobId: text("parent_job_id"), // Reference to parent job (e.g., subtitle removal job -> OCR job)

  status: jobStatusEnum("status").notNull().default(JobsStatus.PENDING),

  step: jobStepEnum("step").notNull().default(JobStep.PREPROCESSING),

  error: text("error"),

  // Batch progress tracking
  totalBatches: integer("total_batches").notNull().default(0),
  batchesCompleted: integer("batches_completed").notNull().default(0),
  submittedImages: integer("submitted_images").notNull().default(0),

  totalImages: integer("total_images").notNull().default(0),
  processedImages: integer("processed_images").notNull().default(0),

  createdAt,
  updatedAt,
});

export type InsertOcrJob = typeof ocrJobs.$inferInsert;
export type SelectOcrJob = typeof ocrJobs.$inferSelect;
export type UpdateOcrJob = Partial<InsertOcrJob>;

// Resultados por frame / recorte
export const ocrJobFrames = pgTable("ocr_job_frames", {
  ocrJobFrameId: createIdField({ name: "ocr_job_frame_id" }),

  jobId: text("job_id")
    .notNull()
    .references(() => ocrJobs.jobId, {
      onDelete: "cascade",
    }),

  filename: text("filename").notNull(),
  baseKey: text("base_key").notNull(), // 3, 3-1 → "3"
  index: integer("index").notNull(),   // orden del frame en el batch

  text: text("text").notNull(),

  createdAt,
  updatedAt,
});

export type InsertOcrJobFrame = typeof ocrJobFrames.$inferInsert;
export type SelectOcrJobFrame = typeof ocrJobFrames.$inferSelect;
export type UpdateOcrJobFrame = Partial<InsertOcrJobFrame>;

// Job items - all files associated with a job (zips, documents, thumbnails)
export const ocrJobItems = pgTable("ocr_job_items", {
  ocrJobItemId: createIdField({ name: "ocr_job_item_id" }),

  jobId: text("job_id")
    .notNull()
    .references(() => ocrJobs.jobId, {
      onDelete: "cascade",
    }),

  itemType: jobItemTypeEnum("item_type").notNull(),

  storageKey: text("storage_key").notNull(), // Key in storage (S3/R2)

  sizeBytes: bigint("size_bytes", { mode: "number" }), // File size in bytes

  contentType: text("content_type"), // MIME type

  parentItemId: text("parent_item_id"), // Reference to parent item (e.g., thumbnail -> zip)

  createdAt,
  updatedAt,
});

export type InsertOcrJobItem = typeof ocrJobItems.$inferInsert;
export type SelectOcrJobItem = typeof ocrJobItems.$inferSelect;
export type UpdateOcrJobItem = Partial<InsertOcrJobItem>;