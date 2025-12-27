export type OpenAIBatchStatus = "validating" | "in_progress" | "completed" | "failed" | "cancelled" | string;

export type OpenAIBatchRetrieveResult = {
  status: OpenAIBatchStatus;
  output_file_id?: string | null;
  error_file_id?: string | null;
  // Optional surfaces depending on SDK/version.
  status_details?: { error?: { message?: string } };
  last_error?: { message?: string };
};

export type OpenAIFileContent = {
  arrayBuffer: () => Promise<ArrayBuffer>;
};

export type OpenAIClientLike = {
  batches: {
    retrieve: (batchId: string) => Promise<OpenAIBatchRetrieveResult>;
  };
  files: {
    content: (fileId: string) => Promise<OpenAIFileContent>;
  };
};

export type CheckedBatchStatus = {
  status: OpenAIBatchStatus;
  outputFileId: string | null;
  errorFileId: string | null;
  failureReason: string | null;
};

export const isEnqueuedTokenLimitFailure = (message: string): boolean => {
  const normalized = message.toLowerCase();
  return (
    normalized.includes("enqueued token limit") ||
    normalized.includes("token limit reached") ||
    normalized.includes("enqueued tokens")
  );
};

export const extractFailureReason = (latest: OpenAIBatchRetrieveResult): string | null => {
  return latest.status_details?.error?.message ?? latest.last_error?.message ?? null;
};

export const readErrorFileFirstLine = async (params: {
  openai: OpenAIClientLike;
  errorFileId: string;
}): Promise<string> => {
  const stream = await params.openai.files.content(params.errorFileId);
  const buf = Buffer.from(await stream.arrayBuffer());
  const text = buf.toString("utf8");
  const first = text
    .split("\n")
    .map((l) => l.trim())
    .find((l) => l.length > 0);
  return first ?? "";
};

export const checkBatchStatus = async (params: {
  openai: OpenAIClientLike;
  batchId: string;
}): Promise<CheckedBatchStatus> => {
  const latest = await params.openai.batches.retrieve(params.batchId);
  const failureReason = extractFailureReason(latest);
  return {
    status: latest.status,
    outputFileId: latest.output_file_id ?? null,
    errorFileId: latest.error_file_id ?? null,
    failureReason,
  };
};

export const getBatchFailureMessage = async (params: {
  openai: OpenAIClientLike;
  errorFileId: string | null;
  failureReason: string | null;
}): Promise<string> => {
  if (params.failureReason) return params.failureReason;
  if (!params.errorFileId) return "";
  try {
    return await readErrorFileFirstLine({ openai: params.openai, errorFileId: params.errorFileId });
  } catch {
    return "";
  }
};

export const nextReducedBatchSize = (params: {
  currentBatchSize: number;
  reductionSteps: readonly number[];
}): number => {
  const idx = params.reductionSteps.indexOf(params.currentBatchSize);
  if (idx === -1) return params.currentBatchSize;
  if (idx >= params.reductionSteps.length - 1) return params.currentBatchSize;
  return params.reductionSteps[idx + 1]!;
};


