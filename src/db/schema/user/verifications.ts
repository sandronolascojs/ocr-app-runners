import { index, pgTable, text, timestamp } from 'drizzle-orm/pg-core';
import { createdAt, createIdField, updatedAt } from '../utils';

export const verifications = pgTable('verifications', {
  id: createIdField({ name: 'id' }),
  identifier: text('identifier').notNull(),
  value: text('value').notNull(),
  expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
  createdAt,
  updatedAt,
}, (table) =>[
  index('verifications_identifier_idx').on(table.identifier),
  index('verifications_expires_at_idx').on(table.expiresAt),
]);
