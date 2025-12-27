import { describe, it, expect } from "vitest";

import {
  MIN_BATCH_SIZE,
  adjustBatchSizeOnTokenError,
  describeError,
  getStatusCode,
  isRateLimitError,
  isServerError,
  isTokenLimitError,
} from "./batchHelpers";

const chunkArray = <T>(arr: T[], size: number): T[][] => {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
};

describe("batchHelpers", () => {
  it("adjustBatchSizeOnTokenError halves and respects minimum", () => {
    expect(adjustBatchSizeOnTokenError(400, 50)).toBe(200);
    expect(adjustBatchSizeOnTokenError(51, 50)).toBe(50);
    expect(adjustBatchSizeOnTokenError(50, 50)).toBe(50);
    expect(adjustBatchSizeOnTokenError(1, 50)).toBe(50);
  });

  it("isTokenLimitError detects 413 and context messages", () => {
    expect(isTokenLimitError({ status: 413 })).toBe(true);
    expect(isTokenLimitError(new Error("context_length_exceeded"))).toBe(true);
    expect(isTokenLimitError(new Error("too many tokens"))).toBe(true);
    expect(isTokenLimitError(new Error("other error"))).toBe(false);
  });

  it("isRateLimitError detects 429", () => {
    expect(isRateLimitError({ status: 429 })).toBe(true);
    expect(isRateLimitError({ statusCode: 429 })).toBe(true);
    expect(isRateLimitError({ status: 500 })).toBe(false);
  });

  it("isServerError detects 5xx", () => {
    expect(isServerError({ status: 500 })).toBe(true);
    expect(isServerError({ statusCode: 503 })).toBe(true);
    expect(isServerError({ status: 429 })).toBe(false);
  });

  it("getStatusCode reads status and $metadata.httpStatusCode", () => {
    expect(getStatusCode({ status: 401 })).toBe(401);
    expect(getStatusCode({ statusCode: 404 })).toBe(404);
    expect(getStatusCode({ $metadata: { httpStatusCode: 502 } })).toBe(502);
    expect(getStatusCode({})).toBeNull();
  });

  it("describeError includes status when present", () => {
    expect(describeError({ status: 429 })).toBe("status=429 message=[object Object]");
    expect(describeError(new Error("boom"))).toBe("boom");
  });

  it("MIN_BATCH_SIZE is enforced in helper", () => {
    expect(adjustBatchSizeOnTokenError(MIN_BATCH_SIZE - 10)).toBe(MIN_BATCH_SIZE);
  });

  it("simulates token-limit shrink and rechunk end-to-end", () => {
    // Scenario: 450 crops, start at batch size 400 -> token fail -> shrink to 200 and continue.
    const totalCrops = 450;
    let currentBatchSize = 400;
    let pendingIndex = 0;
    let processedItems = 0;
    let batchesCompleted = 0;
    let cropsChunks = chunkArray(Array.from({ length: totalCrops }), currentBatchSize);

    // Map of failures by batchIndex at first attempt
    const tokenFailAtBatch: Record<number, boolean> = { 0: true };

    while (pendingIndex < cropsChunks.length) {
      const batchIndex = batchesCompleted + pendingIndex;
      const chunk = cropsChunks[pendingIndex];

      const shouldTokenFail = tokenFailAtBatch[batchIndex];
      if (shouldTokenFail) {
        // Simulate token-limit failure and rechunk remaining
        currentBatchSize = adjustBatchSizeOnTokenError(currentBatchSize);
        const remaining = cropsChunks.slice(pendingIndex).flat();
        cropsChunks = chunkArray(remaining, currentBatchSize);
        pendingIndex = 0;
        continue;
      }

      // success
      processedItems += chunk.length;
      batchesCompleted += 1;
      pendingIndex += 1;
    }

    expect(currentBatchSize).toBe(200); // shrunk once
    expect(batchesCompleted).toBe(3); // 200 + 200 + 50
    expect(processedItems).toBe(totalCrops);
  });
});

