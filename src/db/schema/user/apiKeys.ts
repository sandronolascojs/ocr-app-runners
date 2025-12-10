import { boolean, pgTable, text, uniqueIndex } from "drizzle-orm/pg-core"
import { users } from "./users"
import { apiKeyProviderEnum } from "../utils/enums"
import { createdAt, createIdField, updatedAt } from "../utils"

export const apiKeys = pgTable(
  "api_keys",
  {
    id: createIdField({ name: "id" }),
    userId: text("user_id")
      .notNull()
      .references(() => users.id, { onDelete: "cascade" }),
    provider: apiKeyProviderEnum("provider").notNull(),
    encryptedKey: text("encrypted_key").notNull(),
    keyPrefix: text("key_prefix").notNull(),
    keySuffix: text("key_suffix").notNull(),
    isActive: boolean("is_active").notNull().default(true),
    createdAt,
    updatedAt,
  },
  (table) => [
    uniqueIndex("api_keys_user_provider_active_unique").on(
      table.userId,
      table.provider
    ),
  ]
)

export type InsertApiKey = typeof apiKeys.$inferInsert
export type SelectApiKey = typeof apiKeys.$inferSelect
export type UpdateApiKey = Partial<
  Pick<InsertApiKey, "provider" | "encryptedKey" | "keyPrefix" | "keySuffix" | "isActive" | "updatedAt">
>

