import { AI_CONSTANTS } from "@/constants/ai.constants";

export const INITIAL_BACKOFF_MS = 2_000;
export const MAX_BACKOFF_MS = 30_000;

const messageIncludes = (message: string, patterns: string[]): boolean =>
  patterns.some((p) => message.toLowerCase().includes(p.toLowerCase()));

export const getStatusCode = (error: unknown): number | null => {
  const maybeStatus =
    typeof error === "object" && error !== null
      ? (error as { status?: number; statusCode?: number })?.status ??
        (error as { statusCode?: number })?.statusCode
      : null;
  if (typeof maybeStatus === "number") return maybeStatus;
  const metadataStatus =
    typeof error === "object" &&
    error !== null &&
    "$metadata" in error &&
    typeof (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode ===
      "number"
      ? (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode ?? null
      : null;
  return metadataStatus ?? null;
};

export const isTokenLimitError = (error: unknown): boolean => {
  const status = getStatusCode(error);
  const message = error instanceof Error ? error.message : String(error);
  return (
    status === 413 ||
    messageIncludes(message, [
      "context_length_exceeded",
      "maximum context length",
      "context length",
      "too many tokens",
      "token limit",
      "max_tokens",
    ])
  );
};

export const isRateLimitError = (error: unknown): boolean => getStatusCode(error) === 429;

export const isServerError = (error: unknown): boolean => {
  const status = getStatusCode(error);
  return status !== null && status >= 500;
};

export const describeError = (error: unknown): string => {
  const status = getStatusCode(error);
  const message = error instanceof Error ? error.message : String(error);
  return status ? `status=${status} message=${message}` : message;
};

export const adjustBatchSizeOnTokenError = (
  current: number,
  min = AI_CONSTANTS.BATCH.MIN_SIZE,
  steps: readonly number[] = AI_CONSTANTS.BATCH.SIZE_STEPS
): number => {
  if (current <= min) return min;
  
  // Find the next step down from current size
  // If current is 400, it should go to 300; if 300, go to 200, etc.
  // Steps are ordered from highest to lowest: [400, 300, 200, 100, 50]
  for (let i = 0; i < steps.length; i++) {
    // If current is greater than or equal to this step, find the next lower step
    if (current >= steps[i]) {
      // Find the next step down (if available)
      for (let j = i + 1; j < steps.length; j++) {
        if (steps[j] >= min) {
          return steps[j];
        }
      }
      // If no lower step found, return minimum
      return min;
    }
  }
  
  // If current is smaller than all steps, return minimum
  return min;
};

