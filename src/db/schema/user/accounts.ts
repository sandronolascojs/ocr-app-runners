import { pgTable, text, timestamp, unique } from 'drizzle-orm/pg-core';
import { users } from './users';
import { createdAt, createIdField, updatedAt } from '../utils';

export const accounts = pgTable('accounts', {
  id: createIdField({ name: 'id' }),
  accountId: text('account_id').notNull(),
  providerId: text('provider_id').notNull(),
  userId: text('user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'cascade' }),
  accessToken: text('access_token'),
  refreshToken: text('refresh_token'),
  idToken: text('id_token'),
  accessTokenExpiresAt: timestamp('access_token_expires_at', { withTimezone: true }),
  refreshTokenExpiresAt: timestamp('refresh_token_expires_at', { withTimezone: true }),
  scope: text('scope'),
  password: text('password'),
  createdAt,
  updatedAt,
}, (table) => ({
  accountProviderUnique: unique('user_accounts_account_provider_unique').on(table.accountId, table.providerId),
}));
