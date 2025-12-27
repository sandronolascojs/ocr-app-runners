import { describe, expect, it, vi } from "vitest";
import { Readable } from "node:stream";
import * as fs from "node:fs/promises";

import {
  downloadObjectToTempFileVerifiedWithClient,
} from "./r2";

const streamFromBuffer = (buf: Buffer): Readable => {
  return Readable.from(buf);
};

describe("downloadObjectToTempFileVerifiedWithClient", () => {
  it("writes bytes to temp file and verifies size", async () => {
    const payload = Buffer.from("hello-world");
    const client = {
      send: vi.fn(async () => ({ Body: streamFromBuffer(payload) })),
    };

    const acquire = vi.fn(async () => ({ release: vi.fn() }));
    const result = await downloadObjectToTempFileVerifiedWithClient(
      client,
      { key: "k", prefix: "t" },
      { getSize: async () => payload.length, acquire }
    );

    const st = await fs.stat(result.tempPath);
    expect(st.size).toBe(payload.length);

    await fs.unlink(result.tempPath).catch(() => undefined);
    result.release();
  });

  it("retries when byte count mismatches", async () => {
    const payload = Buffer.from("hello-world");
    // Buffer.slice() is deprecated in newer Node typings; use subarray().
    const tooShort = Buffer.from(payload.subarray(0, payload.length - 1));

    let call = 0;
    const client = {
      send: vi.fn(async () => {
        call += 1;
        return { Body: streamFromBuffer(call === 1 ? tooShort : payload) };
      }),
    };

    const release = vi.fn();
    const acquire = vi.fn(async () => ({ release }));

    const result = await downloadObjectToTempFileVerifiedWithClient(
      client,
      { key: "k", prefix: "t" },
      { getSize: async () => payload.length, acquire }
    );

    expect(client.send).toHaveBeenCalledTimes(2);

    await fs.unlink(result.tempPath).catch(() => undefined);
    result.release();
  });
});


