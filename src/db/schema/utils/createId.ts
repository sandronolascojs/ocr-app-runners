import { text } from "drizzle-orm/pg-core";
import { createId } from "@paralleldrive/cuid2";

export const createIdField = ({ name }: { name: string }) => text(name).primaryKey().$defaultFn(() => createId()).notNull().unique();
