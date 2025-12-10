import type { SelectOcrJobFrame } from "@/db/schema/jobs";
import { getBaseKeyFromFilename } from "@/utils/ocr";

type FrameLike = Pick<SelectOcrJobFrame, "filename" | "baseKey" | "index" | "text">;

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

export const buildParagraphsFromFrames = (frames: FrameLike[]): string[] => {
  if (!frames.length) {
    return [];
  }

  const grouped = new Map<string, FrameLike[]>();

  for (const frame of frames) {
    const normalizedText = frame.text?.trim();
    if (!normalizedText) {
      continue;
    }

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

  return Array.from(grouped.keys())
    .sort(sortKeys)
    .map((key) => {
      const bucket = grouped.get(key)!;
      bucket.sort((a, b) => a.index - b.index);
      return bucket.map((item) => item.text).join(" ");
    });
};

