// Vitest setup: provide dummy env vars required by env schema at import-time.
process.env.DATABASE_URL ??= "postgres://user:pass@localhost:5432/test";
process.env.API_KEY_ENCRYPTION_SECRET ??= "test-secret";
process.env.CLOUDFLARE_R2_ACCOUNT_ID ??= "test-account";
process.env.CLOUDFLARE_R2_ACCESS_KEY_ID ??= "test-access-key";
process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY ??= "test-secret-access-key";
process.env.CLOUDFLARE_R2_BUCKET_NAME ??= "test-bucket";


