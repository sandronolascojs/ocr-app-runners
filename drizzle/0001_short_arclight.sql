CREATE TYPE "public"."job_item_type" AS ENUM('ORIGINAL_ZIP', 'RAW_ZIP', 'CROPPED_ZIP', 'TXT_DOCUMENT', 'DOCX_DOCUMENT', 'THUMBNAIL', 'CROPPED_THUMBNAIL');--> statement-breakpoint
CREATE TABLE "ocr_job_items" (
	"ocr_job_item_id" text PRIMARY KEY NOT NULL,
	"job_id" text NOT NULL,
	"item_type" "job_item_type" NOT NULL,
	"storage_key" text NOT NULL,
	"size_bytes" bigint,
	"content_type" text,
	"parent_item_id" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "ocr_job_items_ocr_job_item_id_unique" UNIQUE("ocr_job_item_id")
);
--> statement-breakpoint
ALTER TABLE "ocr_job_items" ADD CONSTRAINT "ocr_job_items_job_id_ocr_jobs_job_id_fk" FOREIGN KEY ("job_id") REFERENCES "public"."ocr_jobs"("job_id") ON DELETE cascade ON UPDATE no action;