import { describe, it, expect } from "vitest";

import {
  describeError,
  getStatusCode,
  isRateLimitError,
  isServerError,
} from "@/utils/errorHelpers";

describe("error handling helpers", () => {
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
});

