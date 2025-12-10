import { relations } from 'drizzle-orm';
import { pgTable, text, unique } from 'drizzle-orm/pg-core';
import { users } from './users';
import { createdAt, createIdField, updatedAt, teamRoleEnum } from '../utils';
import { TeamRole } from '@/types/enums/teamRole.enum';

export const teams = pgTable('teams', {
  id: createIdField({ name: 'id' }),
  name: text('name').notNull(),
  ownerId: text('owner_id')
    .notNull()
    .unique()
    .references(() => users.id, { onDelete: 'cascade' }),
  createdAt,
  updatedAt,
});

export const teamMembers = pgTable('team_members', {
  id: createIdField({ name: 'id' }),
  teamId: text('team_id')
    .notNull()
    .references(() => teams.id, { onDelete: 'cascade' }),
  userId: text('user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'cascade' }),
  role: teamRoleEnum('role').notNull().default(TeamRole.MEMBER),
  createdAt,
  updatedAt,
}, (table) => [unique('team_members_unique_team_user').on(table.teamId, table.userId),
]);

export const teamRelations = relations(teams, ({ one, many }) => ({
  owner: one(users, {
    fields: [teams.ownerId],
    references: [users.id],
  }),
  members: many(teamMembers),
}));

export const teamMemberRelations = relations(teamMembers, ({ one }) => ({
  team: one(teams, {
    fields: [teamMembers.teamId],
    references: [teams.id],
  }),
  user: one(users, {
    fields: [teamMembers.userId],
    references: [users.id],
  }),
}));

export type InsertTeam = typeof teams.$inferInsert;
export type SelectTeam = typeof teams.$inferSelect;
export type InsertTeamMember = typeof teamMembers.$inferInsert;
export type SelectTeamMember = typeof teamMembers.$inferSelect;

