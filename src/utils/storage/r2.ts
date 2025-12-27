import { createWriteStream, createReadStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import * as fsPromises from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { PassThrough, Transform, type Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  type GetObjectCommandInput,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { Upload } from "@aws-sdk/lib-storage";

import { env } from "@/config/env.config";
import { acquireZipSpool, getZipSpoolSnapshot } from "@/utils/spool/zipSpool";

const r2Endpoint =
  env.CLOUDFLARE_R2_S3_ENDPOINT ??
  `https://${env.CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;

const r2Client = new S3Client({
  region: "auto",
  endpoint: r2Endpoint,
  forcePathStyle: true,
  maxAttempts: 10,
  credentials: {
    accessKeyId: env.CLOUDFLARE_R2_ACCESS_KEY_ID,
    secretAccessKey: env.CLOUDFLARE_R2_SECRET_ACCESS_KEY,
  },
});

type SignedUrlBase = {
  url: string;
  expiresAt: string;
  key: string;
};

export type SignedUploadUrl = SignedUrlBase & {
  method: "PUT";
  headers: Record<string, string>;
};

export type SignedDownloadUrl = SignedUrlBase & {
  headers: Record<string, string>;
};

const createExpiryIso = (ttlSeconds: number): string =>
  new Date(Date.now() + ttlSeconds * 1000).toISOString();

const DEFAULT_STREAM_UPLOAD_CACHE_CONTROL = "private, max-age=0, must-revalidate";

/**
 * Sanitizes a filename for use in Content-Disposition header.
 * Removes CR/LF characters and escapes quotes and backslashes.
 */
const sanitizeFilename = (filename: string): string => {
  return filename
    .replace(/[\r\n]/g, " ") // Replace CR/LF with spaces
    .replace(/\\/g, "\\\\") // Escape backslashes
    .replace(/"/g, '\\"'); // Escape quotes
};

/**
 * Encodes a filename according to RFC5987 for use in Content-Disposition header.
 * Returns the encoded value for the filename* parameter.
 */
const encodeFilenameRfc5987 = (filename: string): string => {
  return encodeURIComponent(filename);
};

/**
 * Checks if an error indicates that an R2 object was not found.
 * Handles various error formats from AWS SDK S3-compatible APIs.
 */
const isNotFoundError = (error: unknown): boolean => {
  const isNotFoundName =
    typeof error === "object" &&
    error !== null &&
    "name" in error &&
    (error as { name?: string }).name === "NotFound";
  const isNotFoundCode =
    typeof error === "object" &&
    error !== null &&
    "Code" in error &&
    (error as { Code?: string }).Code === "NoSuchKey";
  const isNotFoundStatus =
    typeof error === "object" &&
    error !== null &&
    "$metadata" in error &&
    Boolean(
      (error as { $metadata?: { httpStatusCode?: number } }).$metadata
        ?.httpStatusCode === 404
    );

  return isNotFoundName || isNotFoundCode || isNotFoundStatus;
};

export const createSignedUploadUrl = async (params: {
  key: string;
  contentType: string;
}): Promise<SignedUploadUrl> => {
  const ttl = env.R2_SIGNED_UPLOAD_TTL_SECONDS;
  const command = new PutObjectCommand({
    Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
    Key: params.key,
    ContentType: params.contentType,
  });

  const url = await getSignedUrl(r2Client, command, { expiresIn: ttl });

  return {
    key: params.key,
    url,
    method: "PUT",
    headers: {
      "Content-Type": params.contentType,
    },
    expiresAt: createExpiryIso(ttl),
  };
};

export const createSignedDownloadUrl = async (params: {
  key: string;
  responseContentType: string;
  downloadFilename: string;
}): Promise<SignedDownloadUrl> => {
  const ttl = env.R2_SIGNED_DOWNLOAD_TTL_SECONDS;
  const sanitizedFilename = sanitizeFilename(params.downloadFilename);
  const encodedFilename = encodeFilenameRfc5987(params.downloadFilename);
  const contentDisposition = `attachment; filename="${sanitizedFilename}"; filename*=UTF-8''${encodedFilename}`;

  const command = new GetObjectCommand({
    Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
    Key: params.key,
    ResponseContentType: params.responseContentType,
    ResponseContentDisposition: contentDisposition,
  } satisfies GetObjectCommandInput);

  const url = await getSignedUrl(r2Client, command, { expiresIn: ttl });

  return {
    key: params.key,
    url,
    headers: {},
    expiresAt: createExpiryIso(ttl),
  };
};

export const createSignedDownloadUrlWithTtl = async (params: {
  key: string;
  responseContentType: string;
  downloadFilename: string;
  ttlSeconds: number;
}): Promise<SignedDownloadUrl> => {
  const sanitizedFilename = sanitizeFilename(params.downloadFilename);
  const encodedFilename = encodeFilenameRfc5987(params.downloadFilename);
  const contentDisposition = `attachment; filename="${sanitizedFilename}"; filename*=UTF-8''${encodedFilename}`;

  const command = new GetObjectCommand({
    Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
    Key: params.key,
    ResponseContentType: params.responseContentType,
    ResponseContentDisposition: contentDisposition,
  } satisfies GetObjectCommandInput);

  const url = await getSignedUrl(r2Client, command, {
    expiresIn: params.ttlSeconds,
  });

  return {
    key: params.key,
    url,
    headers: {},
    expiresAt: createExpiryIso(params.ttlSeconds),
  };
};

export const ensureObjectExists = async (key: string): Promise<boolean> => {
  try {
    await r2Client.send(
      new HeadObjectCommand({
        Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
        Key: key,
      })
    );
    return true;
  } catch (error) {
    if (isNotFoundError(error)) {
      return false;
    }
    throw error;
  }
};

export const downloadObjectToFile = async (params: {
  key: string;
  filePath: string;
}): Promise<void> => {
  const response = await r2Client.send(
    new GetObjectCommand({
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: params.key,
    })
  );

  if (!response.Body) {
    throw new Error(`R2 object ${params.key} has no body to download.`);
  }

  const readable = response.Body as Readable;
  await mkdir(path.dirname(params.filePath), { recursive: true });
  await pipeline(readable, createWriteStream(params.filePath));
};

export const downloadObjectStream = async (key: string): Promise<Readable> => {
  const response = await r2Client.send(
    new GetObjectCommand({
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: key,
    })
  );

  if (!response.Body) {
    throw new Error(`R2 object ${key} has no body to download.`);
  }

  return response.Body as Readable;
};

const sleep = async (ms: number): Promise<void> => {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
};

const jitterMs = (baseMs: number): number => {
  const jitter = Math.floor(baseMs * (0.2 + Math.random() * 0.3)); // 20%-50%
  return baseMs + jitter;
};

const isTransientStreamError = (error: unknown): boolean => {
  const message = error instanceof Error ? error.message : String(error);
  // Conservative: treat common network disruptions as retryable.
  return (
    message.includes("ECONNRESET") ||
    message.includes("socket hang up") ||
    message.includes("ETIMEDOUT") ||
    message.includes("EPIPE") ||
    message.includes("502") ||
    message.includes("503") ||
    message.includes("504")
  );
};

const randomId = (): string => {
  // small, non-crypto id for temp file names
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
};

const safeUnlink = async (filePath: string): Promise<void> => {
  try {
    await fsPromises.unlink(filePath);
  } catch {
    // ignore
  }
};

export const downloadObjectToTempFileVerified = async (params: {
  key: string;
  prefix?: string;
}): Promise<{ tempPath: string; sizeBytes: number; release: () => void }> => {
  const sizeBytes = await getObjectSize(params.key);
  if (typeof sizeBytes !== "number" || sizeBytes <= 0) {
    throw new Error(
      `Cannot spool ZIP: missing/invalid Content-Length for key="${params.key}".`
    );
  }

  const lease = await acquireZipSpool(sizeBytes);
  const snap = getZipSpoolSnapshot();
  console.log(
    `[zip-spool] acquired bytes=${sizeBytes} inUseBytes=${snap.inUseBytes}/${snap.budgetBytes} inFlight=${snap.inFlight} queued=${snap.queued}`
  );

  const filename = `${params.prefix ?? "zip"}-${randomId()}.zip`;
  const tempPath = path.join(os.tmpdir(), filename);

  const maxAttempts = 5;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    let written = 0;
    try {
      const response = await r2Client.send(
        new GetObjectCommand({
          Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
          Key: params.key,
        })
      );

      if (!response.Body) {
        throw new Error(`R2 object ${params.key} has no body to download.`);
      }

      const readable = response.Body as Readable;
      const counter = new Transform({
        transform(chunk, _encoding, callback) {
          written += chunk.length;
          callback(null, chunk);
        },
      });

      await pipeline(readable, counter, createWriteStream(tempPath));

      if (written !== sizeBytes) {
        throw new Error(
          `Spool byte mismatch: expected=${sizeBytes}, actual=${written}`
        );
      }

      // quick sanity: file exists and matches size
      const st = await fsPromises.stat(tempPath);
      if (st.size !== sizeBytes) {
        throw new Error(
          `Spool file size mismatch on disk: expected=${sizeBytes}, actual=${st.size}`
        );
      }

      return {
        tempPath,
        sizeBytes,
        release: () => {
          lease.release();
          const s = getZipSpoolSnapshot();
          console.log(
            `[zip-spool] released bytes=${sizeBytes} inUseBytes=${s.inUseBytes}/${s.budgetBytes} inFlight=${s.inFlight} queued=${s.queued}`
          );
        },
      };
    } catch (error) {
      console.warn(
        `[zip-spool] download attempt ${attempt}/${maxAttempts} failed key="${params.key}" expected=${sizeBytes} written=${written}:`,
        error instanceof Error ? error.message : error
      );
      await safeUnlink(tempPath);
      if (attempt === maxAttempts) {
        lease.release();
        throw error;
      }
      await sleep(jitterMs(Math.min(10_000, 500 * 2 ** (attempt - 1))));
    }
  }

  // unreachable
  lease.release();
  throw new Error("Failed to spool ZIP to temp file.");
};

export const withTempFileFromR2 = async <T>(params: {
  key: string;
  prefix: string;
  fn: (tempPath: string) => Promise<T>;
}): Promise<T> => {
  const { tempPath, release } = await downloadObjectToTempFileVerified({
    key: params.key,
    prefix: params.prefix,
  });

  let succeeded = false;
  try {
    const result = await params.fn(tempPath);
    succeeded = true;
    return result;
  } finally {
    if (!succeeded && env.R2_SPOOL_KEEP_FILES_ON_ERROR) {
      console.warn(`[zip-spool] keeping temp file for inspection: ${tempPath}`);
    } else {
      await fsPromises.unlink(tempPath).catch(() => undefined);
    }
    release();
  }
};

/**
 * Resumable streaming download for very large objects.
 *
 * Why: piping a single long-lived GetObject stream into unzipper is fragile.
 * Any mid-stream network hiccup truncates the ZIP and causes `unexpected end of file`.
 *
 * This helper re-issues ranged GetObject requests from the last confirmed byte offset
 * and presents a continuous stream to consumers (unzipper).
 */
export const downloadObjectStreamResumableWindowed = async (params: {
  key: string;
  expectedSizeBytes?: number;
  windowBytes?: number;
}): Promise<Readable> => {
  const expectedFromHead = await getObjectSize(params.key);
  const expectedSizeBytes =
    params.expectedSizeBytes ?? expectedFromHead ?? null;

  if (typeof expectedSizeBytes !== "number" || expectedSizeBytes <= 0) {
    throw new Error(
      `Cannot start resumable download: missing/invalid object size for key="${params.key}".`
    );
  }

  const windowBytes =
    params.windowBytes ??
    Math.max(1, env.R2_RANGE_WINDOW_MIB) * 1024 * 1024;
  const maxRetriesPerWindow = env.R2_RANGE_MAX_RETRIES;

  const out = new PassThrough();

  void (async () => {
    let offset = 0;

    while (offset < expectedSizeBytes) {
      const requestStartOffset = offset;
      const requestEndOffset = Math.min(
        expectedSizeBytes - 1,
        requestStartOffset + windowBytes - 1
      );
      const expectedBytesThisWindow = requestEndOffset - requestStartOffset + 1;
      const range: string = `bytes=${requestStartOffset}-${requestEndOffset}`;
      let retries = 0;

      while (true) {
        try {
          const response = await r2Client.send(
            new GetObjectCommand({
              Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
              Key: params.key,
              Range: range,
            })
          );

          if (!response.Body) {
            throw new Error(`R2 object ${params.key} has no body to download.`);
          }

          // Validate Content-Range strictly: start must match requested start and total must match object size.
          if (response.ContentRange) {
            const match: RegExpMatchArray | null = response.ContentRange.match(
              /^bytes\s+(\d+)-(\d+)\/(\d+|\*)$/i
            );
            if (!match) {
              throw new Error(
                `Unexpected Content-Range format: "${response.ContentRange}"`
              );
            }
            const start = Number(match[1]);
            const end = Number(match[2]);
            const total: number | null =
              match[3] === "*" ? null : Number(match[3]);

            if (!Number.isFinite(start) || start !== requestStartOffset) {
              throw new Error(
                `Content-Range start mismatch. expected=${requestStartOffset}, actual=${start}, contentRange="${response.ContentRange}"`
              );
            }
            if (!Number.isFinite(end) || end !== requestEndOffset) {
              throw new Error(
                `Content-Range end mismatch. expected=${requestEndOffset}, actual=${end}, contentRange="${response.ContentRange}"`
              );
            }
            if (
              typeof total === "number" &&
              Number.isFinite(total) &&
              total !== expectedSizeBytes
            ) {
              throw new Error(
                `Content-Range total mismatch. expected=${expectedSizeBytes}, actual=${total}, contentRange="${response.ContentRange}"`
              );
            }
          }

          const readable = response.Body as Readable;
          let bytesThisRequest = 0;

          for await (const chunk of readable) {
            const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
            bytesThisRequest += buf.length;
            offset += buf.length;

            if (!out.write(buf)) {
              await new Promise<void>((resolve) => out.once("drain", resolve));
            }
          }

          if (bytesThisRequest !== expectedBytesThisWindow) {
            // Critical invariant: we must deliver the exact bytes for this window or retry.
            throw new Error(
              `Truncated window. key="${params.key}" range="${range}" expectedBytes=${expectedBytesThisWindow} receivedBytes=${bytesThisRequest}`
            );
          }

          break; // success for this window
        } catch (error) {
          retries += 1;
          const msg = error instanceof Error ? error.message : String(error);
          console.warn(
            `[r2-windowed] error key="${params.key}" range="${range}" offset=${offset}/${expectedSizeBytes} retry=${retries}/${maxRetriesPerWindow}: ${msg}`
          );

          if (!isTransientStreamError(error) && !msg.includes("Truncated window")) {
            out.destroy(
              new Error(
                `Non-retryable download error for key="${params.key}" range="${range}": ${msg}`,
                { cause: error as any }
              )
            );
            return;
          }

          if (retries >= maxRetriesPerWindow) {
            out.destroy(
              new Error(
                `Exceeded max retries for key="${params.key}" range="${range}" at offset=${offset}/${expectedSizeBytes}. LastError=${msg}`,
                { cause: error as any }
              )
            );
            return;
          }

          // Reset offset back to window start before retrying (avoid partial window duplication).
          offset = requestStartOffset;

          // Exponential backoff + jitter.
          const base = Math.min(10_000, 300 * 2 ** Math.min(6, retries - 1));
          await sleep(jitterMs(base));
          continue;
        }
      }

      // Completed a full verified window; loop continues to next window (offset already advanced).
    }

    // Should not happen; if it does, fail loudly.
    out.destroy(
      new Error(
        `Resumable download ended unexpectedly for key="${params.key}". offset=${offset}, expected=${expectedSizeBytes}`
      )
    );
  })();

  return out;
};

/**
 * Lightweight ZIP integrity preflight without disk:
 * fetch the tail (typically contains EOCD) and ensure EOCD signature exists.
 */
export const assertZipTailLooksValid = async (params: {
  key: string;
  expectedSizeBytes?: number;
}): Promise<void> => {
  const expectedFromHead = await getObjectSize(params.key);
  const expectedSizeBytes =
    params.expectedSizeBytes ?? expectedFromHead ?? null;

  if (typeof expectedSizeBytes !== "number" || expectedSizeBytes <= 0) {
    throw new Error(
      `Cannot validate ZIP tail: missing/invalid object size for key="${params.key}".`
    );
  }

  const tailSize = Math.min(128 * 1024, expectedSizeBytes);
  const start = Math.max(0, expectedSizeBytes - tailSize);
  const range = `bytes=${start}-${expectedSizeBytes - 1}`;

  const response = await r2Client.send(
    new GetObjectCommand({
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: params.key,
      Range: range,
    })
  );

  if (!response.Body) {
    throw new Error(`R2 object ${params.key} has no body to download.`);
  }

  const readable = response.Body as Readable;
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of readable) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    chunks.push(buf);
    total += buf.length;
    if (total > 256 * 1024) break;
  }

  const tail = Buffer.concat(chunks, total);
  // EOCD signature: 0x06054b50 (PK\x05\x06)
  const eocdSig = Buffer.from([0x50, 0x4b, 0x05, 0x06]);
  if (tail.indexOf(eocdSig) === -1) {
    throw new Error(
      `ZIP tail validation failed for key="${params.key}". EOCD signature not found in last ${tailSize} bytes.`
    );
  }
};

export const uploadFileToObject = async (params: {
  key: string;
  filePath: string;
  contentType: string;
  cacheControl?: string;
}): Promise<void> => {
  await r2Client.send(
    new PutObjectCommand({
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: params.key,
      Body: createReadStream(params.filePath),
      ContentType: params.contentType,
      CacheControl: params.cacheControl,
    })
  );
};

export const uploadBufferToObject = async (params: {
  key: string;
  body: Buffer;
  contentType: string;
  cacheControl?: string;
}): Promise<void> => {
  await r2Client.send(
    new PutObjectCommand({
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: params.key,
      Body: params.body,
      ContentType: params.contentType,
      CacheControl: params.cacheControl ?? DEFAULT_STREAM_UPLOAD_CACHE_CONTROL,
    })
  );
};

export const uploadStreamToObject = async (params: {
  key: string;
  stream: Readable;
  contentType: string;
  cacheControl?: string;
}): Promise<void> => {
  const uploader = new Upload({
    client: r2Client,
    params: {
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: params.key,
      Body: params.stream,
      ContentType: params.contentType,
      CacheControl: params.cacheControl ?? DEFAULT_STREAM_UPLOAD_CACHE_CONTROL,
    },
    queueSize: 4,
    partSize: 8 * 1024 * 1024, // 8MB parts to balance memory and requests
    leavePartsOnError: false,
  });

  await uploader.done();
};

export const uploadStreamToObjectWithSize = async (params: {
  key: string;
  streamFactory: () => Readable;
  contentType: string;
  cacheControl?: string;
}): Promise<number> => {
  let sizeBytes = 0;

  const countingStream = new Transform({
    transform(chunk, _encoding, callback) {
      sizeBytes += chunk.length;
      callback(null, chunk);
    },
  });

  const upstream = params.streamFactory();
  const passThrough = new PassThrough();

  upstream.pipe(countingStream).pipe(passThrough);

  await uploadStreamToObject({
    key: params.key,
    stream: passThrough,
    contentType: params.contentType,
    cacheControl: params.cacheControl,
  });

  return sizeBytes;
};

export const deleteObjectIfExists = async (key: string): Promise<void> => {
  await r2Client.send(
    new DeleteObjectCommand({
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: key,
    })
  );
};

export const listObjectsByPrefix = async (
  prefix: string
): Promise<Array<{ key: string; size: number }>> => {
  const objects: Array<{ key: string; size: number }> = [];
  let continuationToken: string | undefined;

  do {
    const command = new ListObjectsV2Command({
      Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    });

    const response = await r2Client.send(command);

    if (response.Contents) {
      for (const object of response.Contents) {
        if (object.Key && object.Size !== undefined) {
          objects.push({
            key: object.Key,
            size: object.Size,
          });
        }
      }
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return objects;
};

export const getObjectSize = async (key: string): Promise<number | null> => {
  try {
    const response = await r2Client.send(
      new HeadObjectCommand({
        Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
        Key: key,
      })
    );

    return response.ContentLength ?? null;
  } catch (error) {
    if (isNotFoundError(error)) {
      return null;
    }
    throw error;
  }
};

export const deleteObjectsByPrefix = async (
  prefix: string
): Promise<number> => {
  // Validate prefix to prevent accidental full-bucket deletions
  if (!prefix || prefix.trim().length === 0) {
    throw new Error(
      "Prefix cannot be empty or whitespace-only. This prevents accidental deletion of all objects in the bucket."
    );
  }

  const objects = await listObjectsByPrefix(prefix);
  let deletedCount = 0;

  // Delete in batches of 1000 (S3 limit)
  const batchSize = 1000;
  for (let i = 0; i < objects.length; i += batchSize) {
    const batch = objects.slice(i, i + batchSize);

    try {
      const response = await r2Client.send(
        new DeleteObjectsCommand({
          Bucket: env.CLOUDFLARE_R2_BUCKET_NAME,
          Delete: {
            Objects: batch.map((obj) => ({ Key: obj.key })),
          },
        })
      );

      // Handle errors from the response
      if (response.Errors && response.Errors.length > 0) {
        const failedKeys = response.Errors.map(
          (error) => error.Key ?? "unknown"
        );
        const errorDetails = response.Errors.map(
          (error) => `${error.Key ?? "unknown"}: ${error.Code ?? "unknown"} - ${error.Message ?? "no message"}`
        ).join("; ");

        throw new Error(
          `Failed to delete ${response.Errors.length} object(s) with prefix "${prefix}". Failed keys: ${failedKeys.join(", ")}. Error details: ${errorDetails}`
        );
      }

      // Update deletedCount based on successful deletions returned
      deletedCount += response.Deleted?.length ?? 0;
    } catch (error) {
      // Re-throw with context about which batch failed
      const batchStart = i;
      const batchEnd = Math.min(i + batchSize, objects.length);
      throw new Error(
        `Failed to delete objects batch (indices ${batchStart}-${batchEnd}) with prefix "${prefix}": ${error instanceof Error ? error.message : String(error)}`,
        { cause: error }
      );
    }
  }

  return deletedCount;
};

export const createSignedThumbnailUrl = async (
  key: string
): Promise<SignedDownloadUrl | null> => {
  const exists = await ensureObjectExists(key);
  if (!exists) {
    return null;
  }

  return createSignedDownloadUrl({
    key,
    responseContentType: "image/jpeg",
    downloadFilename: "thumbnail.jpg",
  });
};

