export const MIN_BATCH_SIZE = 50;
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

export const adjustBatchSizeOnTokenError = (current: number, min = MIN_BATCH_SIZE): number => {
  if (current <= min) return min;
  return Math.max(min, Math.floor(current / 2));
};

