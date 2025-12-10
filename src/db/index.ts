import type { ExtractTablesWithRelations } from 'drizzle-orm';
import { drizzle, NeonHttpQueryResultHKT } from 'drizzle-orm/neon-http';
import { PgTransaction } from 'drizzle-orm/pg-core';
import { neon } from '@neondatabase/serverless';
import * as schema from './schema';
import { env } from '@/config/env.config';

const sql = neon(env.DATABASE_URL);
export const db = drizzle({ client: sql, schema });

export type DrizzleDB = typeof db;
export type DrizzleTx = PgTransaction<
  NeonHttpQueryResultHKT,
  typeof schema,
  ExtractTablesWithRelations<typeof schema>
>;
export type DB = DrizzleDB | DrizzleTx;

export * as schema from './schema';
