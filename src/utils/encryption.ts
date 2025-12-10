import { env } from "@/config/env.config"
import { CompactEncrypt, compactDecrypt } from "jose"

/**
 * Minimum length required for the encryption secret (32 characters = 256 bits)
 * This ensures sufficient entropy for secure encryption
 */
const MIN_ENCRYPTION_SECRET_LENGTH = 32

const getEncryptionSecret = (): string => {
  const secret = env.API_KEY_ENCRYPTION_SECRET

  if (!secret) {
    throw new Error(
      `API_KEY_ENCRYPTION_SECRET environment variable is missing or empty. ` +
      `Please set a valid encryption secret with at least ${MIN_ENCRYPTION_SECRET_LENGTH} characters.`
    )
  }

  if (secret.trim().length === 0) {
    throw new Error(
      `API_KEY_ENCRYPTION_SECRET environment variable is empty or contains only whitespace. ` +
      `Please set a valid encryption secret with at least ${MIN_ENCRYPTION_SECRET_LENGTH} characters.`
    )
  }

  if (secret.length < MIN_ENCRYPTION_SECRET_LENGTH) {
    throw new Error(
      `API_KEY_ENCRYPTION_SECRET environment variable is too short (${secret.length} characters). ` +
      `Minimum length required is ${MIN_ENCRYPTION_SECRET_LENGTH} characters for secure encryption. ` +
      `Please update the API_KEY_ENCRYPTION_SECRET environment variable with a longer secret.`
    )
  }

  return secret
}

/**
 * Derives a stable 256-bit key from the secret using SHA-256.
 * We avoid salts and legacy formats to keep encryption deterministic and simple.
 */
const getEncryptionKey = async (secret: string): Promise<CryptoKey> => {
  const encoder = new TextEncoder()
  const secretBytes = encoder.encode(secret)
  const hash = await crypto.subtle.digest("SHA-256", secretBytes)

  return crypto.subtle.importKey(
    "raw",
    hash,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  )
}

/**
 * Encrypts an API key using JWE with AES-256-GCM
 * Generates a cryptographically secure random salt for each encryption
 * Returns the salt (base64url encoded) prepended to the JWE: "{salt}.{jwe}"
 */
export const encryptApiKey = async (
  key: string,
  secret?: string
): Promise<string> => {
  try {
    const encryptionSecret = secret ?? getEncryptionSecret()
    const encryptionKey = await getEncryptionKey(encryptionSecret)
    const jwe = await new CompactEncrypt(new TextEncoder().encode(key))
      .setProtectedHeader({ alg: "dir", enc: "A256GCM" })
      .encrypt(encryptionKey)

    // No salt prefix; output is directly the JWE string
    return jwe
  } catch (error) {
    console.error("Encryption failed: An error occurred during the encryption process")
    throw new Error("Encryption failed")
  }
}

/**
 * Decrypts an API key using JWE
 * Uses the env secret-derived AES-256 key. No salts, no legacy paths.
 */
export const decryptApiKey = async (
  encryptedKey: string,
  secret?: string
): Promise<string> => {
  try {
    const decryptionSecret = secret ?? getEncryptionSecret()
    const decryptionKey = await getEncryptionKey(decryptionSecret)
    const { plaintext } = await compactDecrypt(encryptedKey, decryptionKey)
    return new TextDecoder().decode(plaintext)
  } catch (error) {
    console.error("Decryption failed:", error instanceof Error ? error.message : String(error))
    throw new Error("Failed to decrypt API key")
  }
}

/**
 * Masks an API key to show only prefix and suffix
 * Returns the first 6 characters and last 4 characters
 */
export const maskApiKey = (key: string): { prefix: string; suffix: string } => {
  if (key.length <= 10) {
    // If key is too short, just show dots
    return { prefix: "***", suffix: "***" }
  }

  const prefix = key.substring(0, 6)
  const suffix = key.substring(key.length - 4)

  return { prefix, suffix }
}

