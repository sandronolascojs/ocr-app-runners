import type { SelectOcrJobFrame } from "@/db/schema/jobs";
import { getBaseKeyFromFilename } from "@/utils/ocr";

type FrameLike = Pick<SelectOcrJobFrame, "filename" | "baseKey" | "index" | "text">;

/**
 * Sorts base keys numerically (e.g., "1" < "2" < "10")
 */
const sortKeys = (a: string, b: string): number => {
  const na = Number(a);
  const nb = Number(b);
  const aIsNumber = !Number.isNaN(na);
  const bIsNumber = !Number.isNaN(nb);

  if (aIsNumber && bIsNumber) {
    return na - nb;
  }

  if (aIsNumber) {
    return -1;
  }

  if (bIsNumber) {
    return 1;
  }

  return a.localeCompare(b);
};

/**
 * Extracts numeric parts from filename for sorting.
 * Supports: 1, 1.1, 1.2, 1-1, 1_1, etc.
 * Returns array of numbers for comparison (e.g., [1, 1, 2] for "1.1.2")
 */
const extractNumericParts = (filename: string): number[] => {
  // Remove extension
  const nameWithoutExt = filename.replace(/\.(png|jpe?g)$/i, "");
  // Split by ., -, or _ and extract numbers
  const parts = nameWithoutExt.split(/[._-]/);
  return parts.map((part) => {
    const num = Number.parseInt(part, 10);
    return Number.isNaN(num) ? 0 : num;
  });
};

/**
 * Sorts frames within a baseKey group by their filename numerically.
 * Handles: 1 < 1.1 < 1.2 < 1.3 < 2
 * Also supports: 1 < 1-1 < 1-2 and 1 < 1_1 < 1_2
 */
const sortFramesByFilename = (a: FrameLike, b: FrameLike): number => {
  const aParts = extractNumericParts(a.filename);
  const bParts = extractNumericParts(b.filename);

  // Compare each numeric part
  const maxLength = Math.max(aParts.length, bParts.length);
  for (let i = 0; i < maxLength; i++) {
    const aPart = aParts[i] ?? 0;
    const bPart = bParts[i] ?? 0;
    if (aPart !== bPart) {
      return aPart - bPart;
    }
  }

  // If numeric parts are equal, fall back to string comparison
  return a.filename.localeCompare(b.filename);
};

export const buildParagraphsFromFrames = (frames: FrameLike[]): string[] => {
  if (!frames.length) {
    return [];
  }

  const grouped = new Map<string, FrameLike[]>();

  // Group frames by baseKey
  for (const frame of frames) {
    const normalizedText = frame.text?.trim();
    // Skip frames with empty text
    if (!normalizedText) {
      continue;
    }

    // Determine baseKey: use frame.baseKey if available, otherwise extract from filename
    const baseKey =
      frame.baseKey && frame.baseKey.trim().length > 0
        ? frame.baseKey
        : getBaseKeyFromFilename(frame.filename);
    
    const bucket = grouped.get(baseKey) ?? [];
    bucket.push({
      ...frame,
      baseKey,
      text: normalizedText,
    });
    grouped.set(baseKey, bucket);
  }

  // Build paragraphs: one paragraph per baseKey group
  const paragraphs = Array.from(grouped.keys())
    .sort(sortKeys) // Sort baseKeys numerically: "1" < "2" < "10"
    .map((key) => {
      const bucket = grouped.get(key)!;
      
      // Sort frames within each group by filename numerically
      // This ensures: 1 < 1.1 < 1.2 < 1.3 within the same baseKey group
      bucket.sort(sortFramesByFilename);
      
      // Join all texts from frames in this group into a single paragraph
      const paragraphText = bucket.map((item) => item.text).join(" ");
      
      return paragraphText;
    });

  return paragraphs;
};

