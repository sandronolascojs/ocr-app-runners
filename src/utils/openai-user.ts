import { OpenAI } from "openai"
import { db } from "@/db"
import { apiKeys } from "@/db/schema"
import { eq, and } from "drizzle-orm"
import { ApiKeyProvider } from "@/types/enums/apiKeyProvider.enum"
import { decryptApiKey } from "@/utils/encryption"

/**
 * Gets the user's OpenAI client with their API key
 * Throws an error if the user doesn't have an active API key
 */
export const getUserOpenAIClient = async (
  userId: string
): Promise<OpenAI> => {
  const [activeKey] = await db
    .select()
    .from(apiKeys)
    .where(
      and(
        eq(apiKeys.userId, userId),
        eq(apiKeys.provider, ApiKeyProvider.OPENAI),
        eq(apiKeys.isActive, true)
      )
    )
    .limit(1)

  if (!activeKey) {
    console.error(
      "User does not have an active OpenAI API key configured",
      { userId }
    )
    throw new Error(
      "User does not have an active OpenAI API key configured"
    )
  }

  const decryptedKey = await decryptApiKey(activeKey.encryptedKey)

  return new OpenAI({
    apiKey: decryptedKey,
  })
}

