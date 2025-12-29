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
