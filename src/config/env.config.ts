import { createEnv } from '@t3-oss/env-core';
import { z } from 'zod';

export const env = createEnv({
  server: {
    PORT: z.coerce.number().int().positive().default(3000),
    DATABASE_URL: z.string(),
    API_KEY_ENCRYPTION_SECRET: z.string(),
    CLOUDFLARE_R2_ACCOUNT_ID: z.string(),
    CLOUDFLARE_R2_ACCESS_KEY_ID: z.string(),
    CLOUDFLARE_R2_SECRET_ACCESS_KEY: z.string(),
    CLOUDFLARE_R2_BUCKET_NAME: z.string(),
    CLOUDFLARE_R2_S3_ENDPOINT: z.url().optional(),
    R2_SIGNED_UPLOAD_TTL_SECONDS: z
      .coerce.number()
      .int()
      .positive()
      .default(900),
    R2_SIGNED_DOWNLOAD_TTL_SECONDS: z
      .coerce.number()
      .int()
      .positive()
      .default(900),
    R2_RANGE_WINDOW_MIB: z.coerce.number().int().positive().default(64),
    R2_RANGE_MAX_RETRIES: z.coerce.number().int().positive().default(10),
    R2_SPOOL_BUDGET_GB: z.coerce.number().int().positive().default(50),
    R2_SPOOL_MAX_CONCURRENCY: z.coerce.number().int().positive().default(1),
    R2_SPOOL_KEEP_FILES_ON_ERROR: z.coerce.boolean().default(false),
  },
  runtimeEnv: process.env,
});
