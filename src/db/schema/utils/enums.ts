import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
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

export const apiKeyProviderEnum = pgEnum("api_key_provider", [
  ApiKeyProvider.OPENAI,
]);

export const teamRoleEnum = pgEnum("team_role", [
  TeamRole.OWNER,
  TeamRole.ADMIN,
  TeamRole.MEMBER,
]);