import { env } from "@/config/env.config";

type SpoolLease = {
  bytes: number;
  release: () => void;
};

type QueueItem = {
  bytes: number;
  resolve: (lease: SpoolLease) => void;
  reject: (error: Error) => void;
};

const GB = 1024 * 1024 * 1024;

class ZipSpoolManager {
  private readonly budgetBytes: number;
  private readonly maxConcurrency: number;

  private inUseBytes = 0;
  private inFlight = 0;
  private readonly queue: QueueItem[] = [];

  constructor(params: { budgetGb: number; maxConcurrency: number }) {
    this.budgetBytes = Math.max(1, params.budgetGb) * GB;
    this.maxConcurrency = Math.max(1, params.maxConcurrency);
  }

  public getSnapshot(): {
    budgetBytes: number;
    inUseBytes: number;
    inFlight: number;
    queued: number;
  } {
    return {
      budgetBytes: this.budgetBytes,
      inUseBytes: this.inUseBytes,
      inFlight: this.inFlight,
      queued: this.queue.length,
    };
  }

  public async acquire(bytes: number): Promise<SpoolLease> {
    if (!Number.isFinite(bytes) || bytes <= 0) {
      throw new Error(`Invalid spool bytes requested: ${bytes}`);
    }

    if (bytes > this.budgetBytes) {
      throw new Error(
        `ZIP size (${bytes} bytes) exceeds spool budget (${this.budgetBytes} bytes). Increase R2_SPOOL_BUDGET_GB or use a larger runner volume.`
      );
    }

    return new Promise<SpoolLease>((resolve, reject) => {
      this.queue.push({
        bytes,
        resolve,
        reject,
      });
      this.pump();
    });
  }

  private pump(): void {
    while (this.queue.length > 0) {
      if (this.inFlight >= this.maxConcurrency) return;

      const next = this.queue[0]!;
      const canFit = this.inUseBytes + next.bytes <= this.budgetBytes;
      if (!canFit) return;

      // Dequeue and allocate
      this.queue.shift();
      this.inUseBytes += next.bytes;
      this.inFlight += 1;

      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        this.inUseBytes -= next.bytes;
        this.inFlight -= 1;
        this.pump();
      };

      next.resolve({ bytes: next.bytes, release });
    }
  }
}

const singleton = new ZipSpoolManager({
  budgetGb: env.R2_SPOOL_BUDGET_GB,
  maxConcurrency: env.R2_SPOOL_MAX_CONCURRENCY,
});

// Exported for tests (pure behavior).
export const createZipSpoolManagerForTest = (params: {
  budgetGb: number;
  maxConcurrency: number;
}): {
  acquire: (bytes: number) => Promise<SpoolLease>;
  getSnapshot: () => ReturnType<ZipSpoolManager["getSnapshot"]>;
} => {
  const mgr = new ZipSpoolManager(params);
  return {
    acquire: (bytes) => mgr.acquire(bytes),
    getSnapshot: () => mgr.getSnapshot(),
  };
};

export const acquireZipSpool = async (bytes: number): Promise<SpoolLease> => {
  return singleton.acquire(bytes);
};

export const getZipSpoolSnapshot = (): ReturnType<
  ZipSpoolManager["getSnapshot"]
> => {
  return singleton.getSnapshot();
};


