import { describe, expect, it } from "vitest";
import { createZipSpoolManagerForTest } from "./zipSpool";

describe("zipSpool", () => {
  it("rejects requests larger than budget", async () => {
    const mgr = createZipSpoolManagerForTest({ budgetGb: 1, maxConcurrency: 1 });
    await expect(mgr.acquire(2 * 1024 * 1024 * 1024)).rejects.toThrow(
      "exceeds spool budget"
    );
  });

  it("enforces concurrency and budget with queueing", async () => {
    const mgr = createZipSpoolManagerForTest({ budgetGb: 1, maxConcurrency: 1 });
    const bytes = 512 * 1024 * 1024;

    const lease1 = await mgr.acquire(bytes);
    const snap1 = mgr.getSnapshot();
    expect(snap1.inFlight).toBe(1);
    expect(snap1.inUseBytes).toBe(bytes);

    let acquired2 = false;
    const p2 = mgr.acquire(bytes).then((lease2) => {
      acquired2 = true;
      return lease2;
    });

    // With maxConcurrency=1, the second should not acquire yet.
    await new Promise((r) => setTimeout(r, 10));
    expect(acquired2).toBe(false);

    lease1.release();
    const lease2 = await p2;
    expect(acquired2).toBe(true);
    lease2.release();

    const snapEnd = mgr.getSnapshot();
    expect(snapEnd.inUseBytes).toBe(0);
    expect(snapEnd.inFlight).toBe(0);
  });
});


