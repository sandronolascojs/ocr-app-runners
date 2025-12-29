import { bigint, boolean, index, integer, pgTable, text, uniqueIndex } from "drizzle-orm/pg-core";
import {
  createdAt,
  createIdField,
  jobStatusEnum,
  jobStepEnum,
  jobItemTypeEnum,
  jobTypeEnum,
  ocrBatchKindEnum,
  ocrBatchStatusEnum,
  ocrCropStatusEnum,
  updatedAt,
} from "./utils";
import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { JobType } from "@/types/enums/jobs/jobType.enum";
import { OcrBatchKind } from "@/types/enums/jobs/ocrBatchKind.enum";
import { OcrBatchStatus } from "@/types/enums/jobs/ocrBatchStatus.enum";
import { OcrCropStatus } from "@/types/enums/jobs/ocrCropStatus.enum";

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
export const ocrJobFrames = pgTable(
  "ocr_job_frames",
  {
    ocrJobFrameId: createIdField({ name: "ocr_job_frame_id" }),

    jobId: text("job_id")
      .notNull()
      .references(() => ocrJobs.jobId, {
        onDelete: "cascade",
      }),

    filename: text("filename").notNull(),
    baseKey: text("base_key").notNull(), // 3, 3-1 → "3"
    index: integer("index").notNull(), // orden global del crop (estable)

    text: text("text").notNull(),

    createdAt,
    updatedAt,
  },
  (table) => [
    uniqueIndex("ocr_job_frames_job_id_index_unique").on(table.jobId, table.index),
    index("ocr_job_frames_job_id_idx").on(table.jobId),
  ]
);

export type InsertOcrJobFrame = typeof ocrJobFrames.$inferInsert;
export type SelectOcrJobFrame = typeof ocrJobFrames.$inferSelect;
export type UpdateOcrJobFrame = Partial<InsertOcrJobFrame>;

export const ocrJobCrops = pgTable(
  "ocr_job_crops",
  {
    ocrJobCropId: createIdField({ name: "ocr_job_crop_id" }),

    jobId: text("job_id")
      .notNull()
      .references(() => ocrJobs.jobId, { onDelete: "cascade" }),

    index: integer("index").notNull(),
    filename: text("filename").notNull(),
    baseKey: text("base_key").notNull(),
    cropKey: text("crop_key").notNull(),

    status: ocrCropStatusEnum("status").notNull().default(OcrCropStatus.UPLOADED),
    lastError: text("last_error"),

    createdAt,
    updatedAt,
  },
  (table) => [
    uniqueIndex("ocr_job_crops_job_id_index_unique").on(table.jobId, table.index),
    uniqueIndex("ocr_job_crops_job_id_filename_unique").on(table.jobId, table.filename),
    index("ocr_job_crops_job_id_status_index_idx").on(table.jobId, table.status, table.index),
  ]
);

export type InsertOcrJobCrop = typeof ocrJobCrops.$inferInsert;
export type SelectOcrJobCrop = typeof ocrJobCrops.$inferSelect;
export type UpdateOcrJobCrop = Partial<InsertOcrJobCrop>;

export const ocrJobBatches = pgTable(
  "ocr_job_batches",
  {
    ocrJobBatchId: createIdField({ name: "ocr_job_batch_id" }),

    jobId: text("job_id")
      .notNull()
      .references(() => ocrJobs.jobId, { onDelete: "cascade" }),

    batchNo: integer("batch_no").notNull(),
    startIndex: integer("start_index").notNull(),
    endIndexExclusive: integer("end_index_exclusive").notNull(),
    batchSize: integer("batch_size").notNull(),

    kind: ocrBatchKindEnum("kind").notNull().default(OcrBatchKind.PRIMARY),
    parentBatchId: text("parent_batch_id"),

    openaiBatchId: text("openai_batch_id"),
    openaiInputFileId: text("openai_input_file_id"),
    openaiOutputFileId: text("openai_output_file_id"),
    openaiErrorFileId: text("openai_error_file_id"),

    status: ocrBatchStatusEnum("status").notNull().default(OcrBatchStatus.CREATED),
    failureReason: text("failure_reason"),
    tokenLimitDetected: boolean("token_limit_detected").notNull().default(false),

    createdAt,
    updatedAt,
  },
  (table) => [
    uniqueIndex("ocr_job_batches_job_id_batch_no_unique").on(table.jobId, table.batchNo),
    index("ocr_job_batches_job_id_status_idx").on(table.jobId, table.status),
    index("ocr_job_batches_job_id_start_idx").on(table.jobId, table.startIndex),
  ]
);

export type InsertOcrJobBatch = typeof ocrJobBatches.$inferInsert;
export type SelectOcrJobBatch = typeof ocrJobBatches.$inferSelect;
export type UpdateOcrJobBatch = Partial<InsertOcrJobBatch>;

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