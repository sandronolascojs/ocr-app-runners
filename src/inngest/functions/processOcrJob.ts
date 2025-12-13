import * as fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import archiver from "archiver";
import { Transform } from "node:stream";
import unzipper from "unzipper";
import { inngest } from "@/inngest/client";
import { db } from "@/db";
import { ocrJobs, ocrJobFrames } from "@/db/schema";
import { eq } from "drizzle-orm";
import {
  validateProcessableImageEntry,
  getBaseKeyFromFilename,
  compareImageFilenames,
  extractTextFromCompletion,
  normalizeBufferTo1280x720,
  cropSubtitleFromBuffer,
  createThumbnailFromBuffer,
  type ChatCompletionContent,
} from "@/utils/ocr";
import {
  getJobRootDir,
  getJobRawDir,
  getJobNormalizedDir,
  getJobCropsDir,
  getJobTxtPath,
  getJobDocxPath,
  getJobBatchJsonlPath,
  getJobRawArchivePath,
  getJobZipPath,
  VOLUME_DIRS,
} from "@/utils/paths";
import { writeDocxFromParagraphs } from "@/utils/ocr/docx";
import { buildParagraphsFromFrames } from "@/utils/ocr/paragraphs";
import { JobsStatus } from "@/types";
import { JobStep } from "@/types/enums/jobs/jobStep.enum";
import { InngestEvents, OcrStepId, OcrSleepId } from "@/types/enums/inngest";
import { getUserOpenAIClient } from "@/utils/openai-user";
import type { OpenAI } from "openai";
import { InngestFunctions } from "@/types/enums/inngest/inngestFunctions.enum";
import { AI_CONSTANTS } from "@/constants/ai.constants";
import {
  getJobDocxKey,
  getJobRawArchiveKey,
  getJobTxtKey,
  getJobCropKey,
  getJobThumbnailKey,
  uploadFileToObject,
  uploadBufferToObject,
  uploadStreamToObject,
  createSignedDownloadUrlWithTtl,
  downloadObjectStream,
} from "@/utils/storage";

const BATCH_SLEEP_INTERVAL = "20s";
const BATCH_SIZE = 500; // Reduced from 1000 to avoid token limit errors
const BATCH_SIZE_REDUCTION_STEPS = [500, 400, 300, 200, 100, 50]; // Tamaños de batch a probar cuando hay error de token limit

type CropMeta = {
  filename: string;
  cropKey: string;
  cropSignedUrl: string;
};

type CropMetaMap = Record<string, string>;
type ExpectedEntry = {
  customId: string;
  filename: string;
  index: number;
  url: string;
};

type WorkspacePaths = {
  jobRootDir: string;
  rawDir: string;
  normalizedDir: string;
  cropsDir: string;
  txtPath: string;
  docxPath: string;
  batchJsonlPath: string; // Legacy path, now using getJobBatchJsonlPath with batchIndex
  zipPath: string;
  rawArchivePath: string;
};

type StorageKeys = {
  txtKey: string;
  docxKey: string;
  rawZipKey: string;
};

type StreamingArtifacts = {
  totalImages: number;
  rawZipKey: string | null;
  rawZipSizeBytes: number | null;
  thumbnailKey: string | null;
  cropsMetaPath: string;
};

type BatchArtifacts = {
  batchId: string;
  batchInputFileId: string;
  batchIndex: number;
};

type ProcessedBatchResult = {
  batchId: string;
  batchOutputFileId: string;
  batchIndex: number;
};

type PersistableFrame = {
  jobId: string;
  filename: string;
  baseKey: string;
  index: number;
  text: string;
};

const CROP_SIGNED_URL_TTL_SECONDS = 60 * 60 * 24; // 24 hours

const streamAndProcessZip = async ({
  jobId,
  zipKey,
  storageKeys,
  cropsMetaPath,
  onPreprocessProgress,
}: {
  jobId: string;
  zipKey: string;
  storageKeys: StorageKeys;
  cropsMetaPath: string;
  onPreprocessProgress?: (count: number) => Promise<void>;
}): Promise<StreamingArtifacts> => {
  const zipReadable = await downloadObjectStream(zipKey);
  const unzipStream = zipReadable.pipe(unzipper.Parse({ forceStream: true }));

  const archive = archiver("zip", { zlib: { level: 9 } });
  let filteredZipSizeBytes = 0;
  const sizeCounter = new Transform({
    transform(chunk, _encoding, callback) {
      filteredZipSizeBytes += chunk.length;
      callback(null, chunk);
    },
  });

  const archiveOutput = archive.pipe(sizeCounter);
  const filteredZipUploadPromise = uploadStreamToObject({
    key: storageKeys.rawZipKey,
    stream: archiveOutput,
    contentType: "application/zip",
  });

  const cropsMeta: CropMeta[] = [];
  let processedImages = 0;
  let thumbnailKey: string | null = null;

  for await (const entry of unzipStream) {
    if (entry.type === "Directory") {
      entry.autodrain();
      continue;
    }

    const entryName = entry.path;
    const processable = validateProcessableImageEntry(entryName);
    if (!processable) {
      entry.autodrain();
      continue;
    }

    const fileBuffer = await entry.buffer();
    const normalizedBuffer = await normalizeBufferTo1280x720(fileBuffer);
    const cropBuffer = await cropSubtitleFromBuffer(normalizedBuffer);

    // Only include base images (1, 2, 3, etc.) in the final ZIP
    // Skip decimal variants (1.1, 1.2, etc.) from the ZIP
    // Use original image (raw) with original extension
    if (processable.shouldIncludeInZip) {
      // Extract original extension from originalName (e.g., "1.jpg" -> ".jpg")
      const originalExt = processable.originalName.match(/\.(png|jpe?g)$/i)?.[0] || '.png';
      const zipFilename = `${processable.baseName}${originalExt}`;
      archive.append(fileBuffer, { name: zipFilename });
    }

    // Create crop for ALL images (including 1.1, 1.2, etc.) for OCR processing
    // Use original filename to preserve the relationship
    const cropFilename = processable.originalName.replace(
      /\.(png|jpe?g)$/i,
      ".png"
    );
    const cropKey = getJobCropKey(jobId, cropFilename);
    await uploadBufferToObject({
      key: cropKey,
      body: cropBuffer,
      contentType: "image/png",
    });

    const signedCropUrl = await createSignedDownloadUrlWithTtl({
      key: cropKey,
      responseContentType: "image/png",
      downloadFilename: cropFilename,
      ttlSeconds: CROP_SIGNED_URL_TTL_SECONDS,
    });

    cropsMeta.push({
      filename: cropFilename,
      cropKey,
      cropSignedUrl: signedCropUrl.url,
    });

    if (!thumbnailKey) {
      const thumbnailBuffer = await createThumbnailFromBuffer(normalizedBuffer);
      const thumbKey = getJobThumbnailKey(jobId);
      await uploadBufferToObject({
        key: thumbKey,
        body: thumbnailBuffer,
        contentType: "image/jpeg",
        cacheControl: "public, max-age=31536000, immutable",
      });
      thumbnailKey = thumbKey;
    }

    processedImages += 1;
    if (onPreprocessProgress && processedImages % 50 === 0) {
      await onPreprocessProgress(processedImages);
    }
    if (onPreprocessProgress && processedImages % 50 === 0) {
      await onPreprocessProgress(processedImages);
    }
  }

  await archive.finalize();
  await filteredZipUploadPromise;

  const sortedCrops = [...cropsMeta].sort((a, b) => {
    const comparison = compareImageFilenames(a.filename, b.filename);
    if (comparison !== 0) {
      return comparison;
    }
    return a.filename.localeCompare(b.filename);
  });

  await fs.writeFile(cropsMetaPath, JSON.stringify(sortedCrops), "utf8");

  if (onPreprocessProgress) {
    await onPreprocessProgress(processedImages);
  }

  return {
    totalImages: processedImages,
    rawZipKey: processedImages > 0 ? storageKeys.rawZipKey : null,
    rawZipSizeBytes: processedImages > 0 ? filteredZipSizeBytes : null,
    thumbnailKey,
    cropsMetaPath,
  };
};

/**
 * Divides an array into chunks of specified size
 */
const chunkArray = <T>(array: T[], chunkSize: number): T[][] => {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
};

// Solo cuenta cuántas imágenes procesables hay en el ZIP (sin descargar contenido)
const countProcessableImagesInZip = async (zipKey: string): Promise<number> => {
  const zipReadable = await downloadObjectStream(zipKey);
  const unzipStream = zipReadable.pipe(unzipper.Parse({ forceStream: true }));
  let total = 0;
  for await (const entry of unzipStream) {
    if (entry.type === "Directory") {
      entry.autodrain();
      continue;
    }
    const entryName = entry.path;
    const processable = validateProcessableImageEntry(entryName);
    if (processable) {
      total += 1;
    }
    entry.autodrain();
  }
  return total;
};

const createBatchArtifacts = async ({
  jobId,
  cropsMeta,
  batchIndex,
  globalStartIndex,
  openai,
}: {
  jobId: string;
  cropsMeta: CropMeta[];
  batchIndex: number;
  globalStartIndex: number;
  openai: OpenAI;
}): Promise<BatchArtifacts> => {
  if (!cropsMeta.length) {
    throw new Error(
      `No crops found for job ${jobId} batch ${batchIndex} when creating Batch artifacts.`
    );
  }

  const batchJsonlPath = getJobBatchJsonlPath(jobId, batchIndex);
  const jsonlStream = fsSync.createWriteStream(batchJsonlPath, {
    encoding: "utf8",
  });

  // Register error handler immediately to catch errors from write() calls
  const streamPromise = new Promise<void>((resolve, reject) => {
    jsonlStream.on("error", (err) => reject(err));
    jsonlStream.on("finish", () => resolve());
  });

  // Write all lines for this batch chunk
  for (let localIndex = 0; localIndex < cropsMeta.length; localIndex++) {
    const { filename, cropSignedUrl } = cropsMeta[localIndex];
    const globalIndex = globalStartIndex + localIndex;
    const customId = `job-${jobId}-batch-${batchIndex}-frame-${globalIndex}-${filename}`;

    const line = {
      custom_id: customId,
      method: "POST",
      url: "/v1/chat/completions",
      body: {
        model: AI_CONSTANTS.MODELS.OPENAI,
        temperature: 0,
        max_tokens: 1024,
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: AI_CONSTANTS.PROMPTS.OCR },
              {
                type: "image_url",
                image_url: { url: cropSignedUrl },
              },
            ],
          },
        ],
      },
    };

    jsonlStream.write(JSON.stringify(line) + "\n");
  }

  jsonlStream.end();
  await streamPromise;

  const inputFile = await openai.files.create({
    file: fsSync.createReadStream(batchJsonlPath),
    purpose: "batch",
  });

  try {
    const batch = await openai.batches.create({
      input_file_id: inputFile.id,
      endpoint: "/v1/chat/completions",
      completion_window: "24h",
    });

    return {
      batchId: batch.id,
      batchInputFileId: inputFile.id,
      batchIndex,
    };
  } catch (error) {
    // Check if it's a token limit error
    // OpenAI SDK errors can have different structures:
    // - error.message (string)
    // - error.error?.message (nested error object)
    // - error.response?.data?.error?.message (HTTP response error)
    let errorMessage = "";
    
    if (error instanceof Error) {
      errorMessage = error.message;
    } else if (typeof error === "object" && error !== null) {
      // Check for nested error structures
      const err = error as Record<string, unknown>;
      errorMessage = 
        (err.message as string) ||
        (err.error as { message?: string } | undefined)?.message ||
        (err.response as { data?: { error?: { message?: string } } } | undefined)?.data?.error?.message ||
        String(error);
    } else {
      errorMessage = String(error);
    }

    const isTokenLimitError =
      errorMessage.includes("Enqueued token limit") ||
      errorMessage.includes("token limit reached") ||
      errorMessage.includes("enqueued tokens") ||
      errorMessage.toLowerCase().includes("enqueued token");

    if (isTokenLimitError) {
      // Re-throw with a special error type so we can handle it upstream
      throw new Error(`TOKEN_LIMIT_ERROR:${errorMessage}`);
    }

    // Re-throw other errors as-is
    throw error;
  }
};

const checkBatchStatus = async ({
  batchId,
  openai,
}: {
  batchId: string;
  openai: OpenAI;
}): Promise<{
  status: string;
  outputFileId: string | null;
}> => {
  const latestBatch = await openai.batches.retrieve(batchId);

  return {
    status: latestBatch.status,
    outputFileId: latestBatch.output_file_id as string | null,
  };
};

const saveBatchResults = async ({
  jobId,
  processedBatches,
  totalImages,
  openai,
  cropUrlMap,
  cropsMeta,
}: {
  jobId: string;
  processedBatches: ProcessedBatchResult[];
  totalImages: number;
  openai: OpenAI;
  cropUrlMap: CropMetaMap;
  cropsMeta: CropMeta[];
}): Promise<number> => {
  const framesToPersist: PersistableFrame[] = [];
  let totalProcessedLines = 0;
  const failedItems: Array<{ customId: string; filename: string }> = [];
  const expectedEntries: ExpectedEntry[] = cropsMeta.map((c, idx) => ({
    customId: `job-${jobId}-batch-${Math.floor(idx / BATCH_SIZE)}-frame-${idx}-${c.filename}`,
    filename: c.filename,
    index: idx,
    url: c.cropSignedUrl,
  }));
  const expectedMap = new Map(expectedEntries.map((e) => [e.customId, e]));
  const seen = new Set<string>();

  const parseCustomId = (customId: string) => {
    const match = customId.match(/^job-(.+)-batch-(\d+)-frame-(\d+)-(.+)$/);
    if (!match) return null;
    const index = Number.parseInt(match[3], 10);
    if (Number.isNaN(index)) return null;
    const filename = match[4];
    return { filename, index };
  };

  // Process each batch output file
  for (const { batchOutputFileId, batchIndex } of processedBatches) {
    const outputStream = await openai.files.content(batchOutputFileId);
    const outputBuffer = Buffer.from(await outputStream.arrayBuffer());
    const outputJsonl = outputBuffer.toString("utf8");

    const lines = outputJsonl
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);

    if (!lines.length) {
      throw new Error(
        `Batch ${batchIndex} output file is empty for job ${jobId}.`
      );
    }

    for (const line of lines) {
      let parsed: {
        custom_id?: string;
        error?: { message?: string; code?: string };
        response?: {
          body?: {
            choices?: Array<{
              message?: { content?: ChatCompletionContent };
            }>;
          };
        };
      };

      try {
        parsed = JSON.parse(line) as {
          custom_id?: string;
          error?: { message?: string; code?: string };
          response?: {
            body?: {
              choices?: Array<{
                message?: { content?: ChatCompletionContent };
              }>;
            };
          };
        };
      } catch (error) {
        throw new Error(
          `Invalid JSON line in batch ${batchIndex} output: ${(error as Error).message}`
        );
      }

      if (parsed.error) {
        if (parsed.custom_id) {
          const parsedId = parseCustomId(parsed.custom_id);
          if (parsedId) {
            failedItems.push({
              customId: parsed.custom_id,
              filename: parsedId.filename,
            });
            
            // Include failed items in count with empty text - retry will attempt to get real text
            framesToPersist.push({
              jobId,
              filename: parsedId.filename,
              baseKey: getBaseKeyFromFilename(parsedId.filename),
              index: parsedId.index,
              text: "",
            });
            seen.add(parsed.custom_id);
            totalProcessedLines += 1;
          }
        }
        continue;
      }

      const customId = parsed.custom_id;
      if (!customId) {
        continue;
      }

      // Updated regex to match new custom_id format: job-{jobId}-batch-{batchIndex}-frame-{globalIndex}-{filename}
      const parsedId = parseCustomId(customId);
      if (!parsedId) {
        continue;
      }

      const { filename, index } = parsedId;
      seen.add(customId);

      const completion =
        parsed.response?.body?.choices?.[0]?.message?.content;
      const text = extractTextFromCompletion(completion);

      // Include ALL responses, even if empty - don't skip any frames
      // Convert <EMPTY> or empty strings to empty string to ensure all frames are included
      const finalText = (!text || text === "<EMPTY>") ? "" : text;

      framesToPersist.push({
        jobId,
        filename,
        baseKey: getBaseKeyFromFilename(filename),
        index,
        text: finalText,
      });

      totalProcessedLines += 1;
    }
  }

  // Determinar faltantes no vistos
  const missingItems: Array<{ customId: string; filename: string; index: number }> = [];
  for (const [customId, entry] of expectedMap.entries()) {
    if (!seen.has(customId)) {
      missingItems.push({
        customId,
        filename: entry.filename,
        index: entry.index,
      });
    }
  }

  // Retry failed + missing items una vez, en un mini batch
  const retryItems = [
    ...failedItems.map((f) => ({ ...f, index: expectedMap.get(f.customId)?.index ?? 0 })),
    ...missingItems,
  ];

  if (retryItems.length) {
    const retryJsonlPath = getJobBatchJsonlPath(jobId, processedBatches.length) + "-retry";
    const jsonlStream = fsSync.createWriteStream(retryJsonlPath, {
      encoding: "utf8",
    });

    const seenRetry = new Set<string>();
    for (const item of retryItems) {
      if (seenRetry.has(item.customId)) continue;
      seenRetry.add(item.customId);

      const imageUrl = cropUrlMap[item.filename] || expectedMap.get(item.customId)?.url;
      if (!imageUrl) {
        continue;
      }
      const line = {
        custom_id: item.customId,
        method: "POST",
        url: "/v1/chat/completions",
        body: {
          model: AI_CONSTANTS.MODELS.OPENAI,
          temperature: 0,
          max_tokens: 1024,
          messages: [
            {
              role: "user",
              content: [
                { type: "text", text: AI_CONSTANTS.PROMPTS.OCR },
                {
                  type: "image_url",
                  image_url: { url: imageUrl },
                },
              ],
            },
          ],
        },
      };
      jsonlStream.write(JSON.stringify(line) + "\n");
    }

    jsonlStream.end();
    await new Promise<void>((resolve, reject) => {
      jsonlStream.on("finish", () => resolve());
      jsonlStream.on("error", (err) => reject(err));
    });

    const inputFile = await openai.files.create({
      file: fsSync.createReadStream(retryJsonlPath),
      purpose: "batch",
    });

    const batch = await openai.batches.create({
      input_file_id: inputFile.id,
      endpoint: "/v1/chat/completions",
      completion_window: "24h",
    });

    // Poll simple
    const waitSimple = async (): Promise<string> => {
      for (let attempt = 0; attempt < 120; attempt++) {
        const latest = await openai.batches.retrieve(batch.id);
        if (latest.status === "completed" && latest.output_file_id) {
          return latest.output_file_id as string;
        }
        if (latest.status === "failed" || latest.status === "cancelled") {
          throw new Error(`Retry batch failed with status=${latest.status}`);
        }
        await new Promise((r) => setTimeout(r, 5000));
      }
      throw new Error("Retry batch did not complete in time");
    };

    const retryOutputFileId = await waitSimple();

    // Parse retry output
    const outputStream = await openai.files.content(retryOutputFileId);
    const outputBuffer = Buffer.from(await outputStream.arrayBuffer());
    const outputJsonl = outputBuffer.toString("utf8");
    const lines = outputJsonl
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);

    for (const line of lines) {
      let parsed: {
        custom_id?: string;
        error?: { message?: string; code?: string };
        response?: {
          body?: {
            choices?: Array<{
              message?: { content?: ChatCompletionContent };
            }>;
          };
        };
      };
      try {
        parsed = JSON.parse(line) as {
          custom_id?: string;
          error?: { message?: string; code?: string };
          response?: {
            body?: {
              choices?: Array<{
                message?: { content?: ChatCompletionContent };
              }>;
            };
          };
        };
      } catch (error) {
        continue;
      }

      if (parsed.error) {
        continue;
      }

      const customId = parsed.custom_id;
      if (!customId) {
        continue;
      }
      const parsedId = parseCustomId(customId);
      if (!parsedId) {
        continue;
      }

      const completion =
        parsed.response?.body?.choices?.[0]?.message?.content;
      const text = extractTextFromCompletion(completion);

      // Include ALL responses, even if empty - don't skip any frames
      // Convert <EMPTY> or empty strings to empty string to ensure all frames are included
      const finalText = (!text || text === "<EMPTY>") ? "" : text;

      // Update existing frame if it exists (from failed items), otherwise add new one
      const existingFrameIndex = framesToPersist.findIndex(
        (f) => f.index === parsedId.index && f.filename === parsedId.filename
      );
      
      if (existingFrameIndex >= 0) {
        // Update existing frame with retry result
        framesToPersist[existingFrameIndex].text = finalText;
      } else {
        // Add new frame if it doesn't exist
        framesToPersist.push({
          jobId,
          filename: parsedId.filename,
          baseKey: getBaseKeyFromFilename(parsedId.filename),
          index: parsedId.index,
          text: finalText,
        });
        totalProcessedLines += 1;
      }
    }
  }

  if (totalImages > 0 && totalProcessedLines !== totalImages) {
    throw new Error(
      `Batch output mismatch: expected ${totalImages} responses but got ${totalProcessedLines} across ${processedBatches.length} batches.`
    );
  }

  if (!framesToPersist.length) {
    throw new Error(
      `No OCR frames were parsed from the batch outputs (${processedBatches.length} batches).`
    );
  }

  await db.delete(ocrJobFrames).where(eq(ocrJobFrames.jobId, jobId));
  await db.insert(ocrJobFrames).values(framesToPersist);

  await db
    .update(ocrJobs)
    .set({ step: JobStep.DOCS_BUILT })
    .where(eq(ocrJobs.jobId, jobId));

  return totalProcessedLines;
};

const buildDocuments = async ({
  jobId,
  paths,
  storageKeys,
  totalImages,
}: {
  jobId: string;
  paths: WorkspacePaths;
  storageKeys: StorageKeys;
  totalImages: number;
}): Promise<string | null> => {
  const frames = await db
    .select()
    .from(ocrJobFrames)
    .where(eq(ocrJobFrames.jobId, jobId));

  const paragraphs = buildParagraphsFromFrames(frames);
  if (!paragraphs.length) {
    throw new Error("Unable to build OCR paragraphs for this job.");
  }

  const paragraphsWithBlankLine = paragraphs.flatMap((paragraph, index) =>
    index < paragraphs.length - 1 ? [paragraph, ""] : [paragraph]
  );

  const txtContent = paragraphsWithBlankLine.join("\n");

  await fs.writeFile(paths.txtPath, txtContent, "utf8");
  await writeDocxFromParagraphs(paragraphs, paths.docxPath);

  // Calculate file sizes before uploading
  const txtStats = fsSync.statSync(paths.txtPath);
  const docxStats = fsSync.statSync(paths.docxPath);

  await uploadFileToObject({
    key: storageKeys.txtKey,
    filePath: paths.txtPath,
    contentType: "text/plain; charset=utf-8",
  });

  await uploadFileToObject({
    key: storageKeys.docxKey,
    filePath: paths.docxPath,
    contentType:
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  });

  // Update job with documents info
  await db
    .update(ocrJobs)
    .set({
      status: JobsStatus.DONE,
      txtPath: storageKeys.txtKey,
      docxPath: storageKeys.docxKey,
      txtSizeBytes: txtStats.size,
      docxSizeBytes: docxStats.size,
    })
    .where(eq(ocrJobs.jobId, jobId));

  const dirsToRemove = [paths.rawDir, paths.normalizedDir, paths.cropsDir];
  const filesToRemove = [
    paths.zipPath,
    paths.txtPath,
    paths.docxPath,
    paths.rawArchivePath,
    path.join(paths.cropsDir, "cropsMeta.json"),
  ];

  // Remove all batch JSONL files (legacy and new batch-indexed files)
  // Try to remove the legacy file first
  try {
    await fs.unlink(paths.batchJsonlPath);
  } catch {
    // ignore
  }

  // Remove all batch-indexed files
  // Calculate the number of batches based on totalImages
  const numberOfBatches = Math.ceil(totalImages / BATCH_SIZE);
  // Add a buffer to ensure we clean up all files even if there's a slight mismatch
  const maxBatchesToCheck = numberOfBatches + 10;
  
  for (let i = 0; i < maxBatchesToCheck; i++) {
    try {
      const batchPath = getJobBatchJsonlPath(jobId, i);
      await fs.unlink(batchPath);
    } catch {
      // File doesn't exist or already removed, continue to next
      // Don't break here as batches might not be sequential if there were errors
    }
  }

  for (const file of filesToRemove) {
    try {
      await fs.unlink(file);
    } catch {
      // ignore
    }
  }

  for (const dir of dirsToRemove) {
    try {
      await fs.rm(dir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }

  // Return raw zip key (already saved in DB earlier, use storage key)
  return storageKeys.rawZipKey;
};

const buildWorkspacePaths = (jobId: string): WorkspacePaths => ({
  jobRootDir: getJobRootDir(jobId),
  rawDir: getJobRawDir(jobId),
  normalizedDir: getJobNormalizedDir(jobId),
  cropsDir: getJobCropsDir(jobId),
  txtPath: getJobTxtPath(jobId),
  docxPath: getJobDocxPath(jobId),
  batchJsonlPath: getJobBatchJsonlPath(jobId),
  zipPath: getJobZipPath(jobId),
  rawArchivePath: getJobRawArchivePath(jobId),
});

const buildStorageKeys = (jobId: string): StorageKeys => ({
  txtKey: getJobTxtKey(jobId),
  docxKey: getJobDocxKey(jobId),
  rawZipKey: getJobRawArchiveKey(jobId),
});

const ensureWorkspaceLayout = async (paths: WorkspacePaths) => {
  await fs.mkdir(paths.jobRootDir, { recursive: true });
  await fs.mkdir(paths.rawDir, { recursive: true });
  await fs.mkdir(paths.normalizedDir, { recursive: true });
  await fs.mkdir(paths.cropsDir, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.txtBase, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.wordBase, { recursive: true });
  await fs.mkdir(VOLUME_DIRS.tmpBase, { recursive: true });
};

type ProgressState = {
  totalImages: number;
  processedImages: number; // completadas en preprocesado (crops/resize)
  submittedImages: number; // enviadas a OpenAI
  totalBatches: number;
  batchesCompleted: number;
};

const buildProgress = (overrides?: Partial<ProgressState>): ProgressState => ({
  totalImages: 0,
  processedImages: 0,
  submittedImages: 0,
  totalBatches: 0,
  batchesCompleted: 0,
  ...overrides,
});

const persistProgress = async (
  jobId: string,
  progress: ProgressState,
  extra?: Record<string, unknown>
) => {
  await db
    .update(ocrJobs)
    .set({
      processedImages: progress.processedImages,
      totalImages: progress.totalImages,
      totalBatches: progress.totalBatches,
      batchesCompleted: progress.batchesCompleted,
      submittedImages: progress.submittedImages,
      ...extra,
    })
    .where(eq(ocrJobs.jobId, jobId));
};

// --- Helpers de flujo ---


export const processOcrJob = inngest.createFunction(
  {
    id: InngestFunctions.PROCESS_OCR_JOB,
    timeouts: {
      finish: "2h", // Maximum allowed timeout for processing large batches of images
    },
  },
  { event: InngestEvents.ZIP_UPLOADED },
  async ({ event, step }): Promise<{
    jobId: string;
    txtKey: string;
    docxKey: string;
    rawZipKey: string | null;
  }> => {
    const { jobId, zipKey, userId } = event.data as {
      jobId: string;
      zipKey: string;
      userId: string;
    };

    if (!userId) {
      console.error("UserId missing in event data", event.data);
      
      try {
        await db
          .update(ocrJobs)
          .set({
            status: JobsStatus.ERROR,
            error: "UserId missing in event data",
          })
          .where(eq(ocrJobs.jobId, jobId));
      } catch (updateError) {
        console.error(
          `Failed to update job ${jobId} to ERROR state:`,
          updateError
        );
      }
      
      return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
    }

    try {
      // Get user's OpenAI client
      const openai = await getUserOpenAIClient(userId);

      const [job] = await db
        .select()
        .from(ocrJobs)
        .where(eq(ocrJobs.jobId, jobId))
        .limit(1);

      let progress = buildProgress({
        totalImages: job.totalImages ?? 0,
        processedImages: job.processedImages ?? 0,
        submittedImages: job.submittedImages ?? 0,
        totalBatches: job.totalBatches ?? 0,
        batchesCompleted: job.batchesCompleted ?? 0,
      });

      if (!job) {
        console.error("Job not found", jobId);
        return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
      }

      const storageZipKey = zipKey ?? job.zipPath;
      if (!storageZipKey) {
        console.error("Zip key missing for job", jobId);
        return { jobId, txtKey: "", docxKey: "", rawZipKey: null };
      }

      // Estado actual en memoria (se irá actualizando manualmente)
      let currentStep: JobStep = job.step ?? JobStep.PREPROCESSING;
      let totalImages = job.totalImages ?? 0;

      const workspacePaths = buildWorkspacePaths(jobId);
      const storageKeys = buildStorageKeys(jobId);
      const cropsMetaPath = path.join(workspacePaths.cropsDir, "cropsMeta.json");
      let rawZipKeyForJob: string | null = job.rawZipPath ?? null;

      // If already completed, short-circuit (avoid reading missing temp files)
      if (
        (job.step === JobStep.DOCS_BUILT || job.step === JobStep.RESULTS_SAVED) &&
        job.txtPath &&
        job.docxPath
      ) {
        return {
          jobId,
          txtKey: job.txtPath,
          docxKey: job.docxPath,
          rawZipKey: rawZipKeyForJob,
        };
      }

      await ensureWorkspaceLayout(workspacePaths);

      // Paso 1: contar imágenes procesables antes de procesar
      const countedImages = await step.run("count-images", () =>
        countProcessableImagesInZip(storageZipKey)
      );
      totalImages = countedImages;

      progress = buildProgress({
        totalImages,
        processedImages: 0, // OCR completadas
        submittedImages: 0,
        totalBatches: 0,
        batchesCompleted: 0,
      });

      await persistProgress(jobId, progress, {
        step: JobStep.PREPROCESSING,
        status: JobsStatus.PROCESSING,
      });

      // Paso 2: preprocesar (crops, resize, zip filtrado)
      const streamingResult = await step.run(
        OcrStepId.PreprocessImagesAndCrops,
        () =>
          streamAndProcessZip({
            jobId,
            zipKey: storageZipKey,
            storageKeys,
            cropsMetaPath,
            onPreprocessProgress: async (count) => {
              // processedImages refleja avance de preprocesado (crops/resize)
              progress.processedImages = count;
              await persistProgress(jobId, progress, {
                step: JobStep.PREPROCESSING,
                status: JobsStatus.PROCESSING,
              });
            },
          })
      );

      totalImages = streamingResult.totalImages;
      rawZipKeyForJob = streamingResult.rawZipKey;

      progress = buildProgress({
        totalImages,
        processedImages: totalImages, // ya terminó preprocesado
        submittedImages: 0,
        totalBatches: 0,
        batchesCompleted: 0,
      });

      await persistProgress(jobId, progress, {
        rawZipPath: streamingResult.rawZipKey,
        rawZipSizeBytes: streamingResult.rawZipSizeBytes,
        thumbnailKey: streamingResult.thumbnailKey,
        step: JobStep.BATCH_SUBMITTED,
        status: JobsStatus.PROCESSING,
        batchId: null,
        batchInputFileId: null,
        batchOutputFileId: null,
      });

      if (!fsSync.existsSync(cropsMetaPath)) {
        console.warn(
          `cropsMeta.json missing for job ${jobId}; assuming processing already completed or temp cleaned.`
        );
        return {
          jobId,
          txtKey: job.txtPath ?? "",
          docxKey: job.docxPath ?? "",
          rawZipKey: rawZipKeyForJob,
        };
      }

      const cropsMeta: CropMeta[] = JSON.parse(
        fsSync.readFileSync(cropsMetaPath, "utf8")
      ) as CropMeta[];
      if (!cropsMeta.length) {
        throw new Error("No crops were generated from the provided ZIP file.");
      }
      const cropUrlMap: CropMetaMap = Object.fromEntries(
        cropsMeta.map((c) => [c.filename, c.cropSignedUrl])
      );
      currentStep = JobStep.BATCH_SUBMITTED;

      if (currentStep === JobStep.BATCH_SUBMITTED) {
        // Divide cropsMeta into chunks - start with default BATCH_SIZE
        let currentBatchSize = BATCH_SIZE;
        let cropsChunks = chunkArray(cropsMeta, currentBatchSize);
        const processedBatches: ProcessedBatchResult[] = [];
        let totalBatches = cropsChunks.length;
        let batchIndex = 0;
        let processedItemsCount = 0;

        progress.totalBatches = totalBatches;
        await persistProgress(jobId, progress);

        console.log(
          `Processing ${totalImages} images in ${totalBatches} batches of ${currentBatchSize} images each for job ${jobId}`
        );

        // Process each batch chunk sequentially - wait for each batch to complete before starting the next
        // Use while loop to handle dynamic re-chunking when batch size is reduced
        while (batchIndex < cropsChunks.length) {
          const globalStartIndex = processedItemsCount;

          console.log(
            `Creating batch ${batchIndex + 1}/${totalBatches} for job ${jobId} (images ${globalStartIndex + 1}-${globalStartIndex + cropsChunks[batchIndex].length}) with batch size ${currentBatchSize}`
          );

          // Try to create batch with retry logic for token limit errors
          // Wrap the entire retry logic in step.run for idempotency
          const artifacts = await step.run(
            `${OcrStepId.CreateAndAwaitBatch}-${batchIndex}`,
            async () => {
              let result: BatchArtifacts | null = null;
              let retryAttempt = 0;
              let localBatchSize = currentBatchSize;
              let localChunks = [...cropsChunks];
              let localChunk = localChunks[batchIndex];
              
              const currentBatchSizeIndex = BATCH_SIZE_REDUCTION_STEPS.indexOf(localBatchSize);
              const maxRetries = BATCH_SIZE_REDUCTION_STEPS.length - 1 - (currentBatchSizeIndex >= 0 ? currentBatchSizeIndex : 0);

              while (!result && retryAttempt <= maxRetries) {
                try {
                  if (!localChunk || localChunk.length === 0) {
                    throw new Error(`Empty chunk at batch index ${batchIndex}`);
                  }

                  console.log(
                    `Attempting to create batch ${batchIndex + 1} with ${localChunk.length} items (batch size: ${localBatchSize}, retry attempt: ${retryAttempt})`
                  );

                  // Create batch artifacts (JSONL file and OpenAI batch)
                  result = await createBatchArtifacts({
                    jobId,
                    cropsMeta: localChunk,
                    batchIndex,
                    globalStartIndex,
                    openai,
                  });
                } catch (error) {
                  const errorMessage = error instanceof Error ? error.message : String(error);
                  
                  console.error(
                    `Error creating batch ${batchIndex + 1} (attempt ${retryAttempt + 1}):`,
                    errorMessage
                  );

                  if (errorMessage.startsWith("TOKEN_LIMIT_ERROR:") && retryAttempt < maxRetries) {
                    // Find next smaller batch size
                    const currentRetryIndex = BATCH_SIZE_REDUCTION_STEPS.indexOf(localBatchSize);
                    if (currentRetryIndex < BATCH_SIZE_REDUCTION_STEPS.length - 1) {
                      const newBatchSize = BATCH_SIZE_REDUCTION_STEPS[currentRetryIndex + 1];
                      retryAttempt += 1;

                      console.warn(
                        `Token limit error for batch ${batchIndex + 1}. Reducing batch size from ${localBatchSize} to ${newBatchSize} (attempt ${retryAttempt + 1}/${maxRetries + 1})`
                      );

                      // Re-chunk the current chunk with new batch size
                      // Take only the first part of the current chunk
                      const chunkToRetry = localChunk.slice(0, newBatchSize);
                      
                      if (chunkToRetry.length === 0) {
                        throw new Error(
                          `No items in chunk after reducing batch size to ${newBatchSize}`
                        );
                      }

                      // Update local chunk to the smaller size
                      localChunk = chunkToRetry;
                      localBatchSize = newBatchSize;

                      console.log(
                        `Retrying with reduced chunk size: ${chunkToRetry.length} items (from original ${localChunks[batchIndex].length})`
                      );

                      // Continue retry loop with smaller chunk
                      continue;
                    } else {
                      throw new Error(
                        `Token limit error and no smaller batch size available. Current: ${localBatchSize}, Steps: ${BATCH_SIZE_REDUCTION_STEPS.join(", ")}`
                      );
                    }
                  }

                  // If we've exhausted all retry sizes or it's a different error, throw
                  throw error;
                }
              }

              if (!result) {
                throw new Error(
                  `Failed to create batch ${batchIndex} after ${retryAttempt} attempts with different batch sizes.`
                );
              }

              // Update the global state if batch size was reduced
              if (localBatchSize !== currentBatchSize) {
                // Get the original chunk to see what items remain
                const originalChunk = cropsChunks[batchIndex];
                const processedInThisBatch = localChunk.length;
                const remainingInOriginalChunk = originalChunk.slice(processedInThisBatch);
                
                // Re-chunk all remaining items (from this chunk + all subsequent chunks) with new batch size
                const remainingCrops = [
                  ...remainingInOriginalChunk,
                  ...cropsMeta.slice(processedItemsCount + originalChunk.length)
                ];
                const newChunks = chunkArray(remainingCrops, localBatchSize);

                // Replace chunks from current batchIndex onwards with the new smaller chunks
                cropsChunks.splice(batchIndex, cropsChunks.length - batchIndex, ...newChunks);

                // Update current batch size for future batches
                currentBatchSize = localBatchSize;

                // Recalculate total batches
                const remainingItems = cropsMeta.length - processedItemsCount - processedInThisBatch;
                totalBatches = batchIndex + 1 + Math.ceil(remainingItems / localBatchSize);

                progress.totalBatches = totalBatches;
                await persistProgress(jobId, progress);

                console.log(
                  `Batch size reduced to ${localBatchSize}. Processed ${processedInThisBatch} items, re-chunked remaining ${remainingItems} items into ${newChunks.length} batches`
                );
              }

              return result;
            }
          );

          if (!artifacts || !artifacts.batchId) {
            throw new Error(
              `Batch ID missing after creation for batch ${batchIndex}.`
            );
          }

          // Use cropsChunks[batchIndex] to get the actual chunk size (may have been reduced)
          progress.submittedImages += cropsChunks[batchIndex].length;
          await persistProgress(jobId, progress);

          console.log(
            `Waiting for batch ${batchIndex + 1}/${totalBatches} (batchId: ${artifacts.batchId}) to complete for job ${jobId}`
          );

          // Wait for this batch to complete before processing the next one
          // This ensures we don't exceed OpenAI rate limits and process sequentially
          // Use separate steps for each check to allow proper sleep handling
          let batchOutputFileId: string | null = null;
          let attempt = 0;

          while (true) {
            const batchStatus = await step.run(
              `${OcrStepId.WaitBatchCompletion}-${batchIndex}-${attempt}`,
              () =>
                checkBatchStatus({
                  batchId: artifacts.batchId,
                  openai,
                })
            );

            if (
              batchStatus.status === "completed" &&
              batchStatus.outputFileId
            ) {
              batchOutputFileId = batchStatus.outputFileId;
              break;
            }

            if (
              batchStatus.status === "failed" ||
              batchStatus.status === "cancelled"
            ) {
              throw new Error(
                `Batch ${batchIndex} failed with status=${batchStatus.status}`
              );
            }

            // Sleep before next check - this must be outside step.run
            await step.sleep(
              `${OcrSleepId.WaitBatchCompletion}-${jobId}-${batchIndex}-${attempt}`,
              BATCH_SLEEP_INTERVAL
            );
            attempt += 1;
          }

          if (!batchOutputFileId) {
            throw new Error(
              `Batch ${batchIndex} completed but output file ID is missing`
            );
          }

          progress.batchesCompleted += 1;
          await persistProgress(jobId, progress);

          console.log(
            `Batch ${batchIndex + 1}/${totalBatches} completed for job ${jobId}`
          );

          processedBatches.push({
            batchId: artifacts.batchId,
            batchOutputFileId,
            batchIndex,
          });

          // Update counters for next iteration
          // Use cropsChunks[batchIndex] to get the actual chunk size (may have been reduced)
          processedItemsCount += cropsChunks[batchIndex].length;
          batchIndex += 1;

          // Update job with the last batch info (for tracking purposes)
          await db
            .update(ocrJobs)
            .set({
              batchId: artifacts.batchId,
              batchInputFileId: artifacts.batchInputFileId,
              batchOutputFileId,
            })
            .where(eq(ocrJobs.jobId, jobId));
        }

        console.log(
          `All ${totalBatches} batches completed for job ${jobId}. Processing results...`
        );

        await db
          .update(ocrJobs)
          .set({
            step: JobStep.RESULTS_SAVED,
          })
          .where(eq(ocrJobs.jobId, jobId));

        currentStep = JobStep.RESULTS_SAVED;

        // Save all batch results (processedImages se queda para preprocesado)
        await step.run(OcrStepId.SaveResultsToDb, () =>
          saveBatchResults({
            jobId,
            processedBatches,
            totalImages,
            openai,
            cropUrlMap,
            cropsMeta,
          })
        );

        progress.batchesCompleted = totalBatches;
        await persistProgress(jobId, progress, { step: JobStep.RESULTS_SAVED });

        currentStep = JobStep.DOCS_BUILT;
      }

      if (currentStep === JobStep.DOCS_BUILT) {
        rawZipKeyForJob = await step.run(OcrStepId.BuildDocsAndCleanup, () =>
          buildDocuments({
            jobId,
            paths: workspacePaths,
            storageKeys,
            totalImages,
          })
        );

        // Ensure the job reflects the final step in case any prior update was skipped
        await db
          .update(ocrJobs)
          .set({
            step: JobStep.DOCS_BUILT,
            status: JobsStatus.DONE,
          })
          .where(eq(ocrJobs.jobId, jobId));
      }

      return {
        jobId,
        txtKey: storageKeys.txtKey,
        docxKey: storageKeys.docxKey,
        rawZipKey: rawZipKeyForJob,
      };
    } catch (err) {
      console.error("processOcrJob failed", jobId, err);

      const errorMessage =
        err instanceof Error ? err.message : "Unknown error in OCR job";

      // Guardar error y marcar job como ERROR; el retry lo relanza desde el step que quedó
      await db
        .update(ocrJobs)
        .set({
          status: JobsStatus.ERROR,
          error: errorMessage,
        })
        .where(eq(ocrJobs.jobId, jobId));

      throw err;
    }
  }
);