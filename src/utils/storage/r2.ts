import { createWriteStream, createReadStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import path from "node:path";
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

const r2Endpoint =
  env.CLOUDFLARE_R2_S3_ENDPOINT ??
  `https://${env.CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;

const r2Client = new S3Client({
  region: "auto",
  endpoint: r2Endpoint,
  forcePathStyle: true,
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

