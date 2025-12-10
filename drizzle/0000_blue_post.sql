CREATE TYPE "public"."api_key_provider" AS ENUM('openai');--> statement-breakpoint
CREATE TYPE "public"."job_status" AS ENUM('PENDING', 'PROCESSING', 'DONE', 'ERROR');--> statement-breakpoint
CREATE TYPE "public"."ocr_job_step" AS ENUM('PREPROCESSING', 'BATCH_SUBMITTED', 'RESULTS_SAVED', 'DOCS_BUILT');--> statement-breakpoint
CREATE TYPE "public"."team_role" AS ENUM('owner', 'admin', 'member');--> statement-breakpoint
CREATE TABLE "ocr_job_frames" (
	"ocr_job_frame_id" text PRIMARY KEY NOT NULL,
	"job_id" text NOT NULL,
	"filename" text NOT NULL,
	"base_key" text NOT NULL,
	"index" integer NOT NULL,
	"text" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "ocr_job_frames_ocr_job_frame_id_unique" UNIQUE("ocr_job_frame_id")
);
--> statement-breakpoint
CREATE TABLE "ocr_jobs" (
	"ocr_job_id" text PRIMARY KEY NOT NULL,
	"job_id" text NOT NULL,
	"user_id" text NOT NULL,
	"status" "job_status" DEFAULT 'PENDING' NOT NULL,
	"step" "ocr_job_step" DEFAULT 'PREPROCESSING' NOT NULL,
	"error" text,
	"zip_path" text NOT NULL,
	"txt_path" text,
	"docx_path" text,
	"raw_zip_path" text,
	"batch_id" text,
	"batch_input_file_id" text,
	"batch_output_file_id" text,
	"total_images" integer DEFAULT 0 NOT NULL,
	"processed_images" integer DEFAULT 0 NOT NULL,
	"txt_size_bytes" bigint,
	"docx_size_bytes" bigint,
	"raw_zip_size_bytes" bigint,
	"thumbnail_key" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "ocr_jobs_ocr_job_id_unique" UNIQUE("ocr_job_id"),
	CONSTRAINT "ocr_jobs_job_id_unique" UNIQUE("job_id")
);
--> statement-breakpoint
CREATE TABLE "accounts" (
	"id" text PRIMARY KEY NOT NULL,
	"account_id" text NOT NULL,
	"provider_id" text NOT NULL,
	"user_id" text NOT NULL,
	"access_token" text,
	"refresh_token" text,
	"id_token" text,
	"access_token_expires_at" timestamp with time zone,
	"refresh_token_expires_at" timestamp with time zone,
	"scope" text,
	"password" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "accounts_id_unique" UNIQUE("id"),
	CONSTRAINT "user_accounts_account_provider_unique" UNIQUE("account_id","provider_id")
);
--> statement-breakpoint
CREATE TABLE "api_keys" (
	"id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"provider" "api_key_provider" NOT NULL,
	"encrypted_key" text NOT NULL,
	"key_prefix" text NOT NULL,
	"key_suffix" text NOT NULL,
	"is_active" boolean DEFAULT true NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "api_keys_id_unique" UNIQUE("id")
);
--> statement-breakpoint
CREATE TABLE "sessions" (
	"id" text PRIMARY KEY NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"token" text NOT NULL,
	"ip_address" text,
	"user_agent" text,
	"user_id" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "sessions_id_unique" UNIQUE("id"),
	CONSTRAINT "sessions_token_unique" UNIQUE("token")
);
--> statement-breakpoint
CREATE TABLE "team_members" (
	"id" text PRIMARY KEY NOT NULL,
	"team_id" text NOT NULL,
	"user_id" text NOT NULL,
	"role" "team_role" DEFAULT 'member' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "team_members_id_unique" UNIQUE("id"),
	CONSTRAINT "team_members_unique_team_user" UNIQUE("team_id","user_id")
);
--> statement-breakpoint
CREATE TABLE "teams" (
	"id" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"owner_id" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "teams_id_unique" UNIQUE("id"),
	CONSTRAINT "teams_owner_id_unique" UNIQUE("owner_id")
);
--> statement-breakpoint
CREATE TABLE "users" (
	"id" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"email" text NOT NULL,
	"email_verified" boolean NOT NULL,
	"image" text,
	"is_enabled" boolean NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "users_id_unique" UNIQUE("id"),
	CONSTRAINT "users_email_unique" UNIQUE("email")
);
--> statement-breakpoint
CREATE TABLE "verifications" (
	"id" text PRIMARY KEY NOT NULL,
	"identifier" text NOT NULL,
	"value" text NOT NULL,
	"expires_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "verifications_id_unique" UNIQUE("id")
);
--> statement-breakpoint
ALTER TABLE "ocr_job_frames" ADD CONSTRAINT "ocr_job_frames_job_id_ocr_jobs_job_id_fk" FOREIGN KEY ("job_id") REFERENCES "public"."ocr_jobs"("job_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "accounts" ADD CONSTRAINT "accounts_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "api_keys" ADD CONSTRAINT "api_keys_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sessions" ADD CONSTRAINT "sessions_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "team_members" ADD CONSTRAINT "team_members_team_id_teams_id_fk" FOREIGN KEY ("team_id") REFERENCES "public"."teams"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "team_members" ADD CONSTRAINT "team_members_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "teams" ADD CONSTRAINT "teams_owner_id_users_id_fk" FOREIGN KEY ("owner_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "api_keys_user_provider_active_unique" ON "api_keys" USING btree ("user_id","provider");--> statement-breakpoint
CREATE INDEX "verifications_identifier_idx" ON "verifications" USING btree ("identifier");--> statement-breakpoint
CREATE INDEX "verifications_expires_at_idx" ON "verifications" USING btree ("expires_at");