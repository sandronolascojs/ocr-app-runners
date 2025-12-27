import { describe, expect, it } from "vitest";
import * as fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import archiver from "archiver";

import { forEachZipEntry, readStreamToBuffer } from "./zipFile";

const createTempZip = async (entries: Array<{ name: string; body: Buffer }>) => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "zip-test-"));
  const zipPath = path.join(tmpDir, "test.zip");

  const archive = archiver("zip", { zlib: { level: 9 } });
  const out = await fs.open(zipPath, "w");
  const writable = out.createWriteStream();

  const done = new Promise<void>((resolve, reject) => {
    writable.on("close", () => resolve());
    writable.on("error", reject);
    archive.on("error", reject);
  });

  archive.pipe(writable);
  for (const e of entries) {
    archive.append(e.body, { name: e.name });
  }
  await archive.finalize();
  await done;
  await out.close();

  return { zipPath, tmpDir };
};

describe("zipFile", () => {
  it("iterates entries and reads buffers", async () => {
    const { zipPath, tmpDir } = await createTempZip([
      { name: "a.txt", body: Buffer.from("hello") },
      { name: "b.bin", body: Buffer.from([1, 2, 3]) },
    ]);

    const seen: Record<string, Buffer> = {};
    await forEachZipEntry({
      filePath: zipPath,
      onEntry: async (entry) => {
        if (entry.isDirectory) return;
        const s = await entry.openReadStream();
        const buf = await readStreamToBuffer(s, { expectedBytes: entry.uncompressedSize });
        seen[entry.fileName] = buf;
      },
    });

    expect(Object.keys(seen).sort()).toEqual(["a.txt", "b.bin"]);
    expect(seen["a.txt"]!.toString("utf8")).toBe("hello");
    expect([...seen["b.bin"]!]).toEqual([1, 2, 3]);

    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  it("detects truncation when expectedBytes is provided", async () => {
    const { zipPath, tmpDir } = await createTempZip([
      { name: "a.txt", body: Buffer.from("hello") },
    ]);

    let err: unknown = null;
    try {
      await forEachZipEntry({
        filePath: zipPath,
        onEntry: async (entry) => {
          if (entry.fileName !== "a.txt") return;
          const s = await entry.openReadStream();
          await readStreamToBuffer(s, { expectedBytes: entry.uncompressedSize + 1, context: "test" });
        },
      });
    } catch (e) {
      err = e;
    }

    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toContain("ZIP entry processing failed");

    await fs.rm(tmpDir, { recursive: true, force: true });
  });
});


