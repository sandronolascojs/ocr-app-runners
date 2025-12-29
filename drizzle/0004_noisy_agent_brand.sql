CREATE TYPE "public"."ocr_batch_kind" AS ENUM('PRIMARY', 'RETRY');--> statement-breakpoint
CREATE TYPE "public"."ocr_batch_status" AS ENUM('CREATED', 'SUBMITTED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED');--> statement-breakpoint
CREATE TYPE "public"."ocr_crop_status" AS ENUM('UPLOADED', 'PROCESSED', 'FAILED_RETRYABLE', 'FAILED_FINAL');--> statement-breakpoint
CREATE TABLE "ocr_job_batches" (
	"ocr_job_batch_id" text PRIMARY KEY NOT NULL,
	"job_id" text NOT NULL,
	"batch_no" integer NOT NULL,
	"start_index" integer NOT NULL,
	"end_index_exclusive" integer NOT NULL,
	"batch_size" integer NOT NULL,
	"kind" "ocr_batch_kind" DEFAULT 'PRIMARY' NOT NULL,
	"parent_batch_id" text,
	"openai_batch_id" text,
	"openai_input_file_id" text,
	"openai_output_file_id" text,
	"openai_error_file_id" text,
	"status" "ocr_batch_status" DEFAULT 'CREATED' NOT NULL,
	"failure_reason" text,
	"token_limit_detected" boolean DEFAULT false NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "ocr_job_batches_ocr_job_batch_id_unique" UNIQUE("ocr_job_batch_id")
);
--> statement-breakpoint
CREATE TABLE "ocr_job_crops" (
	"ocr_job_crop_id" text PRIMARY KEY NOT NULL,
	"job_id" text NOT NULL,
	"index" integer NOT NULL,
	"filename" text NOT NULL,
	"base_key" text NOT NULL,
	"crop_key" text NOT NULL,
	"status" "ocr_crop_status" DEFAULT 'UPLOADED' NOT NULL,
	"last_error" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "ocr_job_crops_ocr_job_crop_id_unique" UNIQUE("ocr_job_crop_id")
);
--> statement-breakpoint
ALTER TABLE "ocr_job_batches" ADD CONSTRAINT "ocr_job_batches_job_id_ocr_jobs_job_id_fk" FOREIGN KEY ("job_id") REFERENCES "public"."ocr_jobs"("job_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "ocr_job_crops" ADD CONSTRAINT "ocr_job_crops_job_id_ocr_jobs_job_id_fk" FOREIGN KEY ("job_id") REFERENCES "public"."ocr_jobs"("job_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "ocr_job_batches_job_id_batch_no_unique" ON "ocr_job_batches" USING btree ("job_id","batch_no");--> statement-breakpoint
CREATE INDEX "ocr_job_batches_job_id_status_idx" ON "ocr_job_batches" USING btree ("job_id","status");--> statement-breakpoint
CREATE INDEX "ocr_job_batches_job_id_start_idx" ON "ocr_job_batches" USING btree ("job_id","start_index");--> statement-breakpoint
CREATE UNIQUE INDEX "ocr_job_crops_job_id_index_unique" ON "ocr_job_crops" USING btree ("job_id","index");--> statement-breakpoint
CREATE UNIQUE INDEX "ocr_job_crops_job_id_filename_unique" ON "ocr_job_crops" USING btree ("job_id","filename");--> statement-breakpoint
CREATE INDEX "ocr_job_crops_job_id_status_index_idx" ON "ocr_job_crops" USING btree ("job_id","status","index");--> statement-breakpoint
CREATE UNIQUE INDEX "ocr_job_frames_job_id_index_unique" ON "ocr_job_frames" USING btree ("job_id","index");--> statement-breakpoint
CREATE INDEX "ocr_job_frames_job_id_idx" ON "ocr_job_frames" USING btree ("job_id");