import { bigint, integer, pgTable, text } from "drizzle-orm/pg-core";
import { createdAt, createIdField, jobStatusEnum, jobStepEnum, updatedAt } from "./utils";
import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";

export const ocrJobs = pgTable("ocr_jobs", {
  ocrJobId: createIdField({ name: "ocr_job_id" }),

  jobId: text("job_id").notNull().unique(),

  userId: text("user_id").notNull(),

  status: jobStatusEnum("status").notNull().default(JobsStatus.PENDING),

  step: jobStepEnum("step").notNull().default(JobStep.PREPROCESSING),

  error: text("error"),

  zipPath: text("zip_path").notNull(),

  txtPath: text("txt_path"),
  docxPath: text("docx_path"),
  rawZipPath: text("raw_zip_path"),

  // Info del batch de OpenAI
  batchId: text("batch_id"),
  batchInputFileId: text("batch_input_file_id"),
  batchOutputFileId: text("batch_output_file_id"),

  totalImages: integer("total_images").notNull().default(0),
  processedImages: integer("processed_images").notNull().default(0),

  // Storage tracking
  txtSizeBytes: bigint("txt_size_bytes", { mode: "number" }),
  docxSizeBytes: bigint("docx_size_bytes", { mode: "number" }),
  rawZipSizeBytes: bigint("raw_zip_size_bytes", { mode: "number" }),
  thumbnailKey: text("thumbnail_key"),

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