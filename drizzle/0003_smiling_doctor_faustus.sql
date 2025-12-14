CREATE TYPE "public"."job_type" AS ENUM('OCR', 'SUBTITLE_REMOVAL');--> statement-breakpoint
ALTER TABLE "ocr_jobs" ADD COLUMN "job_type" "job_type" DEFAULT 'OCR' NOT NULL;--> statement-breakpoint
ALTER TABLE "ocr_jobs" ADD COLUMN "parent_job_id" text;