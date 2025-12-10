import { relations } from 'drizzle-orm';
import { boolean, pgTable, text } from 'drizzle-orm/pg-core';
import { accounts } from './accounts';
import { sessions } from './sessions';
import { verifications } from './verifications';
import { teams, teamMembers } from './teams';
import { createdAt, createIdField, updatedAt } from '../utils';

export const users = pgTable('users', {
  id: createIdField({ name: 'id' }),
  name: text('name').notNull(),
  email: text('email').notNull().unique(),
  emailVerified: boolean('email_verified')
    .$defaultFn(() => false)
    .notNull(),
  image: text('image'),
  isEnabled: boolean('is_enabled')
    .$defaultFn(() => false)
    .notNull(),
  createdAt,
  updatedAt,
});

export const userRelations = relations(users, ({ one, many }) => ({
  accounts: many(accounts),
  sessions: many(sessions),
  verifications: many(verifications),
  ownedTeams: many(teams),
  teamMemberships: many(teamMembers),
}));

export type InsertUser = typeof users.$inferInsert;
export type SelectUser = typeof users.$inferSelect;
