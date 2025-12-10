import path from "node:path";
import sharp from "sharp";

type CanonicalImageResult = {
  baseName: string;
};

export type ChatCompletionContentPart =
  | string
  | {
      type?: string;
      text?: string;
    };

export type ChatCompletionContent = string | ChatCompletionContentPart[];

type ProcessableImageResult = {
  baseName: string;
  originalName: string;
  shouldIncludeInZip: boolean;
};

/**
 * Filters and canonicalizes image entry names for ZIP processing.
 * Rules:
 * - Only accepts png/jpg/jpeg.
 * - Must start with an integer prefix (e.g., 1, 2, 10). Anything else is dropped.
 * - Only the first occurrence per integer base is kept; subsequent variants (decimals,
 *   hyphenated, duplicates) are discarded.
 * - Filters out macOS metadata (__MACOSX/, ._ prefix).
 */
export const canonicalizeImageEntry = (
  entryName: string,
  usedBases: Set<string>
): CanonicalImageResult | null => {
  // Filter macOS metadata directories
  if (entryName.startsWith("__MACOSX/")) return null;

  const base = path.basename(entryName);
  // Filter AppleDouble files
  if (base.startsWith("._")) return null;

  const ext = path.extname(base);
  if (!/\.(png|jpe?g)$/i.test(ext)) return null;

  const nameWithoutExt = path.basename(base, ext);
  // Only allow pure integer names (e.g., "5", "0003"). Anything with hyphens, dots, or extra chars is dropped.
  if (!/^\d+$/.test(nameWithoutExt)) return null;

  const parsed = Number.parseInt(nameWithoutExt, 10);
  if (Number.isNaN(parsed)) return null;

  const baseName = String(parsed);
  if (usedBases.has(baseName)) {
    return null;
  }

  usedBases.add(baseName);
  return { baseName };
};

/**
 * Validates if an image entry should be processed (for OCR).
 * Accepts images that start with a number (e.g., "1", "1.1", "1.2", "2", "10-1").
 * All valid images are processed for OCR, but only pure integer names go to the final ZIP.
 * Rules:
 * - Only accepts png/jpg/jpeg.
 * - Must start with an integer prefix (e.g., 1, 1.1, 1.2, 2, 10-1). Anything else is dropped.
 * - Filters out macOS metadata (__MACOSX/, ._ prefix).
 * - Returns baseName (first integer), originalName, and whether it should be included in ZIP.
 */
export const validateProcessableImageEntry = (
  entryName: string
): ProcessableImageResult | null => {
  // Filter macOS metadata directories
  if (entryName.startsWith("__MACOSX/")) return null;

  const base = path.basename(entryName);
  // Filter AppleDouble files
  if (base.startsWith("._")) return null;

  const ext = path.extname(base);
  if (!/\.(png|jpe?g)$/i.test(ext)) return null;

  const nameWithoutExt = path.basename(base, ext);
  
  // Must start with at least one digit
  const match = nameWithoutExt.match(/^(\d+)/);
  if (!match) return null;

  const baseName = String(Number.parseInt(match[1], 10));
  
  // Only pure integer names (e.g., "1", "2", "10") should be included in the final ZIP
  // Names like "1.1", "1.2", "2-1" are processed but not included in ZIP
  const shouldIncludeInZip = /^\d+$/.test(nameWithoutExt);

  return {
    baseName,
    originalName: base,
    shouldIncludeInZip,
  };
};

/**
 * Extracts text from OpenAI chat completion content.
 * Handles both string and array formats of completion content.
 */
export const extractTextFromCompletion = (
  completion?: ChatCompletionContent
): string => {
  if (typeof completion === "string") {
    return completion.trim();
  }

  if (Array.isArray(completion)) {
    return completion
      .map((part) => {
        if (typeof part === "string") {
          return part;
        }

        if (
          part &&
          typeof part === "object" &&
          "type" in part &&
          (part as { type?: string }).type === "text" &&
          "text" in part &&
          typeof (part as { text?: string }).text === "string"
        ) {
          return (part as { text: string }).text;
        }

        return "";
      })
      .join("")
      .trim();
  }

  return "";
};

/**
 * Normalizes an image buffer to 1280x720 resolution.
 * Maintains aspect ratio with black bars if needed.
 */
export const normalizeBufferTo1280x720 = async (input: Buffer): Promise<Buffer> => {
  const image = sharp(input);
  const meta = await image.metadata();

  const targetW = 1280;
  const targetH = 720;
  const width = meta.width ?? 0;
  const height = meta.height ?? 0;

  if (!width || !height) {
    return image.resize(targetW, targetH, { fit: "contain" }).png().toBuffer();
  }

  const aspect = width / height;
  const targetAspect = targetW / targetH;

  if (Math.abs(aspect - targetAspect) < 0.01) {
    return image.resize(targetW, targetH).png().toBuffer();
  }

  return image
    .resize(targetW, targetH, {
      fit: "contain",
      background: { r: 0, g: 0, b: 0, alpha: 1 },
    })
    .png()
    .toBuffer();
};

/**
 * Crops the subtitle region (bottom 32%) from a normalized image buffer.
 */
export const cropSubtitleFromBuffer = async (
  normalizedBuffer: Buffer
): Promise<Buffer> => {
  const image = sharp(normalizedBuffer);
  const meta = await image.metadata();

  const width = meta.width ?? 0;
  const height = meta.height ?? 0;

  if (!width || !height) {
    return image.png().toBuffer();
  }

  const roiHeight = Math.floor(height * 0.32);
  const top = Math.max(0, height - roiHeight);

  return image
    .extract({ left: 0, top, width, height: roiHeight })
    .png()
    .toBuffer();
};

/**
 * Creates a thumbnail from an image buffer (200x200, JPEG, quality 85).
 */
export const createThumbnailFromBuffer = async (buffer: Buffer): Promise<Buffer> => {
  return sharp(buffer)
    .resize(200, 200, { fit: "inside", withoutEnlargement: true })
    .jpeg({ quality: 85 })
    .toBuffer();
};

type FilenameToken = number | string;

const tokenizeFilename = (input: string): FilenameToken[] => {
  const name = input.toLowerCase().replace(/\.[^.]+$/, "");
  const rawTokens = name.match(/\d+|[^\d]+/g);

  if (!rawTokens) {
    return [name];
  }

  return rawTokens
    .map((token) => (/\d+/.test(token) ? Number(token) : token))
    .filter((token) => token !== "");
};

export const compareImageFilenames = (a: string, b: string): number => {
  const tokensA = tokenizeFilename(a);
  const tokensB = tokenizeFilename(b);
  const maxLength = Math.max(tokensA.length, tokensB.length);

  for (let index = 0; index < maxLength; index++) {
    const tokenA = tokensA[index];
    const tokenB = tokensB[index];

    if (tokenA === undefined) return -1;
    if (tokenB === undefined) return 1;

    if (typeof tokenA === "number" && typeof tokenB === "number") {
      if (tokenA !== tokenB) return tokenA - tokenB;
      continue;
    }

    if (typeof tokenA === "number") return -1;
    if (typeof tokenB === "number") return 1;

    if (tokenA !== tokenB) {
      return tokenA.localeCompare(tokenB);
    }
  }

  return 0;
};

/**
 * Devuelve la "clave base" numérica de un filename.
 * 3.png → "3", 3-1.png → "3", 3_2.jpeg → "3", 12-3.png → "12".
 */
export function getBaseKeyFromFilename(filename: string): string {
  const name = filename.replace(/\.[^.]+$/, "");
  const match = name.match(/^(\d+)/);
  if (!match) return name;
  return String(parseInt(match[1], 10));
}