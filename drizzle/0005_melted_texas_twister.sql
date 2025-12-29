DROP TABLE "ocr_job_batches" CASCADE;--> statement-breakpoint
DROP TABLE "ocr_job_crops" CASCADE;--> statement-breakpoint
ALTER TABLE "ocr_jobs" ALTER COLUMN "step" SET DATA TYPE text;--> statement-breakpoint
ALTER TABLE "ocr_jobs" ALTER COLUMN "step" SET DEFAULT 'PREPROCESSING'::text;--> statement-breakpoint
DROP TYPE "public"."ocr_job_step";--> statement-breakpoint
CREATE TYPE "public"."ocr_job_step" AS ENUM('PREPROCESSING', 'RESULTS_SAVED', 'DOCS_BUILT');--> statement-breakpoint
ALTER TABLE "ocr_jobs" ALTER COLUMN "step" SET DEFAULT 'PREPROCESSING'::"public"."ocr_job_step";--> statement-breakpoint
ALTER TABLE "ocr_jobs" ALTER COLUMN "step" SET DATA TYPE "public"."ocr_job_step" USING "step"::"public"."ocr_job_step";--> statement-breakpoint
ALTER TABLE "ocr_jobs" DROP COLUMN "total_batches";--> statement-breakpoint
ALTER TABLE "ocr_jobs" DROP COLUMN "batches_completed";--> statement-breakpoint
ALTER TABLE "ocr_jobs" DROP COLUMN "submitted_images";--> statement-breakpoint
DROP TYPE "public"."ocr_batch_kind";--> statement-breakpoint
DROP TYPE "public"."ocr_batch_status";--> statement-breakpoint
DROP TYPE "public"."ocr_crop_status";