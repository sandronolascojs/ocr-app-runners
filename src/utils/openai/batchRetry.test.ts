import { describe, expect, it } from "vitest";
import {
  extractFailureReason,
  getBatchFailureMessage,
  isEnqueuedTokenLimitFailure,
  checkBatchStatus,
  nextReducedBatchSize,
  type OpenAIClientLike,
} from "./batchRetry";

describe("openai batch retry helpers", () => {
  it("detects enqueued token limit failures", () => {
    expect(
      isEnqueuedTokenLimitFailure(
        "Enqueued token limit reached for gpt-4.1 in organization org-123. Limit: 900,000 enqueued tokens."
      )
    ).toBe(true);
    expect(isEnqueuedTokenLimitFailure("some other error")).toBe(false);
  });

  it("extracts failure reason from status_details or last_error", () => {
    expect(
      extractFailureReason({
        status: "failed",
        status_details: { error: { message: "Enqueued token limit reached" } },
      })
    ).toBe("Enqueued token limit reached");

    expect(
      extractFailureReason({
        status: "failed",
        last_error: { message: "Boom" },
      })
    ).toBe("Boom");

    expect(extractFailureReason({ status: "failed" })).toBeNull();
  });

  it("reads failure message from error_file_id when failureReason is missing", async () => {
    const openai: OpenAIClientLike = {
      batches: {
        retrieve: async () => ({ status: "failed" }),
      },
      files: {
        content: async () => ({
          arrayBuffer: async () =>
            Buffer.from('{"error":{"message":"Enqueued token limit reached"}}\n').buffer,
        }),
      },
    };

    const msg = await getBatchFailureMessage({
      openai,
      errorFileId: "file_123",
      failureReason: null,
    });
    expect(msg.length).toBeGreaterThan(0);
  });

  it("checkBatchStatus returns a stable shape", async () => {
    const openai: OpenAIClientLike = {
      batches: {
        retrieve: async () => ({
          status: "failed",
          output_file_id: null,
          error_file_id: "file_123",
          status_details: { error: { message: "Enqueued token limit reached" } },
        }),
      },
      files: {
        content: async () => ({
          arrayBuffer: async () => Buffer.from("x").buffer,
        }),
      },
    };

    const status = await checkBatchStatus({ openai, batchId: "batch_1" });
    expect(status.status).toBe("failed");
    expect(status.outputFileId).toBeNull();
    expect(status.errorFileId).toBe("file_123");
    expect(status.failureReason).toBe("Enqueued token limit reached");
  });

  it("computes next reduced batch size deterministically", () => {
    const steps = [500, 400, 300, 200, 100, 50] as const;
    expect(nextReducedBatchSize({ currentBatchSize: 500, reductionSteps: steps })).toBe(400);
    expect(nextReducedBatchSize({ currentBatchSize: 50, reductionSteps: steps })).toBe(50);
    expect(nextReducedBatchSize({ currentBatchSize: 999, reductionSteps: steps })).toBe(999);
  });
});


