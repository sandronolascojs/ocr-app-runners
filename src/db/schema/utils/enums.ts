import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { JobItemType } from "@/types/enums/jobs/jobItemType.enum";
import { JobType } from "@/types/enums/jobs/jobType.enum";
import { ApiKeyProvider } from "@/types/enums/apiKeyProvider.enum";
import { TeamRole } from "@/types/enums/teamRole.enum";
import { pgEnum } from "drizzle-orm/pg-core";

export const jobStatusEnum = pgEnum("job_status", [
  JobsStatus.PENDING,
  JobsStatus.PROCESSING,
  JobsStatus.DONE,
  JobsStatus.ERROR,
]);

export const jobStepEnum = pgEnum("ocr_job_step", [
  JobStep.PREPROCESSING,
  JobStep.BATCH_SUBMITTED,
  JobStep.RESULTS_SAVED,
  JobStep.DOCS_BUILT,
]);

export const jobItemTypeEnum = pgEnum("job_item_type", [
  JobItemType.ORIGINAL_ZIP,
  JobItemType.RAW_ZIP,
  JobItemType.CROPPED_ZIP,
  JobItemType.TXT_DOCUMENT,
  JobItemType.DOCX_DOCUMENT,
  JobItemType.THUMBNAIL,
  JobItemType.CROPPED_THUMBNAIL,
]);

export const jobTypeEnum = pgEnum("job_type", [
  JobType.OCR,
  JobType.SUBTITLE_REMOVAL,
]);

export const apiKeyProviderEnum = pgEnum("api_key_provider", [
  ApiKeyProvider.OPENAI,
]);

export const teamRoleEnum = pgEnum("team_role", [
  TeamRole.OWNER,
  TeamRole.ADMIN,
  TeamRole.MEMBER,
]);