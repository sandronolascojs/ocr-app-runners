import yauzl from "yauzl";
import type { Entry, ZipFile } from "yauzl";
import { Readable } from "node:stream";

export type ZipEntry = {
  fileName: string;
  uncompressedSize: number;
  isDirectory: boolean;
  openReadStream: () => Promise<Readable>;
};

const openZip = async (filePath: string): Promise<ZipFile> => {
  return new Promise<ZipFile>((resolve, reject) => {
    yauzl.open(
      filePath,
      {
        lazyEntries: true,
        decodeStrings: true,
        validateEntrySizes: true,
      },
      (err, zipfile) => {
        if (err || !zipfile) {
          reject(err ?? new Error("Failed to open zip file"));
          return;
        }
        resolve(zipfile);
      }
    );
  });
};

const openEntryStream = async (
  zipfile: ZipFile,
  entry: Entry
): Promise<Readable> => {
  return new Promise<Readable>((resolve, reject) => {
    zipfile.openReadStream(entry, (err, stream) => {
      if (err || !stream) {
        reject(err ?? new Error("Failed to open zip entry stream"));
        return;
      }
      resolve(stream);
    });
  });
};

export const readStreamToBuffer = async (
  stream: Readable,
  opts?: {
    expectedBytes?: number;
    context?: string;
  }
): Promise<Buffer> => {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of stream) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    chunks.push(buf);
    total += buf.length;
  }

  if (typeof opts?.expectedBytes === "number" && opts.expectedBytes >= 0) {
    if (total !== opts.expectedBytes) {
      const ctx = opts.context ? ` (${opts.context})` : "";
      throw new Error(
        `Truncated zip entry stream${ctx}: expectedBytes=${opts.expectedBytes}, actualBytes=${total}`
      );
    }
  }

  return Buffer.concat(chunks, total);
};

export const forEachZipEntry = async (params: {
  filePath: string;
  onEntry: (entry: ZipEntry) => Promise<void>;
}): Promise<void> => {
  const zipfile = await openZip(params.filePath);

  try {
    await new Promise<void>((resolve, reject) => {
      const next = () => zipfile.readEntry();

      zipfile.once("error", reject);
      zipfile.once("end", resolve);

      zipfile.on("entry", (entry: Entry) => {
        const isDirectory = /\/$/.test(entry.fileName);

        void (async () => {
          try {
            await params.onEntry({
              fileName: entry.fileName,
              uncompressedSize: entry.uncompressedSize,
              isDirectory,
              openReadStream: async () => openEntryStream(zipfile, entry),
            });
            next();
          } catch (err) {
            reject(
              new Error(
                `ZIP entry processing failed: entry="${entry.fileName}", uncompressedSize=${entry.uncompressedSize}, compressedSize=${entry.compressedSize}`,
                { cause: err as any }
              )
            );
          }
        })();
      });

      next();
    });
  } finally {
    zipfile.close();
  }
};


