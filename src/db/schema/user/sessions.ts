import { pgTable, text, timestamp } from 'drizzle-orm/pg-core';
import { createIdField, createdAt, updatedAt } from '../utils';
import { users } from './users';

export const sessions = pgTable('sessions', {
  id: createIdField({ name: 'id' }),
  expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
  token: text('token').notNull().unique(),
  ipAddress: text('ip_address'),
  userAgent: text('user_agent'),
  userId: text('user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'cascade' }),
  createdAt,
  updatedAt,
});
