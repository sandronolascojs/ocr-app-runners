import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import crypto from "node:crypto";

/**
 * Gets a unique instance identifier for multi-instance isolation.
 * Uses INSTANCE_ID environment variable if provided, otherwise generates
 * a cryptographically strong UUID to ensure uniqueness across instances.
 */
function getInstanceId(): string {
  if (process.env.INSTANCE_ID) {
    return process.env.INSTANCE_ID;
  }
  // Generate a cryptographically strong UUID for each instance
  // This ensures unique isolation between different processes/instances
  // and prevents collisions in container/serverless environments
  return crypto.randomUUID();
}

/**
 * Gets the persistent application data directory based on the operating system.
 * - macOS: ~/Library/Application Support/ocr-app
 * - Linux: ~/.local/share/ocr-app
 * - Windows: %APPDATA%/ocr-app
 */
function getPersistentAppDataDir(): string {
  const homeDir = os.homedir();
  const platform = os.platform();

  if (platform === "darwin") {
    return path.join(homeDir, "Library", "Application Support", "ocr-app");
  }
  if (platform === "win32") {
    const appData = process.env.APPDATA || homeDir;
    return path.join(appData, "ocr-app");
  }
  // Linux and other Unix-like systems
  return path.join(homeDir, ".local", "share", "ocr-app");
}

/**
 * Validates that a directory path is valid and accessible.
 * Attempts to create the directory (and parent directories) if they don't exist.
 * @throws Error if the path is invalid or cannot be created
 */
function validateAndEnsureDirectory(dirPath: string): string {
  try {
    // Resolve to absolute path
    const absolutePath = path.resolve(dirPath);

    // Ensure the directory exists (recursive will create parent dirs if needed)
    fs.mkdirSync(absolutePath, { recursive: true });

    // Verify write permissions
    const testFile = path.join(absolutePath, ".write-test");

    // Attempt to write test file - if this fails, directory is not writable
    try {
      fs.writeFileSync(testFile, "test");
    } catch (error) {
      throw new Error(
        `Directory is not writable: ${absolutePath}. Please check permissions.`,
      );
    }

    // Cleanup test file - wrap in try-catch to prevent file leak
    // Unlink failures should not abort successful validation
    try {
      fs.unlinkSync(testFile);
    } catch (unlinkError) {
      // Log unlink failure but don't throw - validation succeeded
      console.warn(
        `Failed to cleanup test file ${testFile}: ${unlinkError instanceof Error ? unlinkError.message : String(unlinkError)}`,
      );
      // If write succeeded but unlink failed, we still have a valid writable directory
      // The test file will remain but validation is still successful
    }

    return absolutePath;
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error(`Failed to validate directory: ${dirPath}`);
  }
}

/**
 * Gets the system temporary directory, respecting standard environment variables.
 * Priority: TMPDIR (Unix) / TEMP (Windows) > TMP > os.tmpdir()
 * This ensures compatibility with serverless environments like Inngest.
 */
function getSystemTempDir(): string {
  // Respect standard environment variables used in serverless environments
  // TMPDIR is standard on Unix/Linux, TEMP on Windows
  const tmpDir =
    process.env.TMPDIR || process.env.TMP || process.env.TEMP || os.tmpdir();
  return tmpDir;
}

/**
 * Gets the base volume directory for OCR operations.
 *
 * Priority:
 * 1. OCR_BASE_DIR environment variable (if set and valid) - explicit configuration
 * 2. System temp directory (ephemeral, cleared on reboot) - DEFAULT for serverless
 *    - Uses TMPDIR/TMP/TEMP environment variables if available (common in serverless)
 *    - Falls back to os.tmpdir() if not set
 * 3. Persistent application data directory (fallback when temp validation fails)
 *
 * @warning The default behavior uses the system temp directory which is EPHEMERAL.
 * Data stored in the temp directory will be lost on system reboot, container restart,
 * or temp cleanup. This is acceptable for serverless environments like Inngest where
 * files are only used temporarily during processing and then uploaded to persistent
 * storage (R2 in this case).
 *
 * For production deployments requiring data persistence between executions, set
 * OCR_BASE_DIR to a persistent storage location (e.g., mounted volume, EFS, etc.).
 *
 * The base path includes instance isolation to prevent conflicts in multi-instance
 * deployments. Each instance gets its own subdirectory.
 */
function getBaseVolumeDir(): string {
  let baseDir: string;
  let usePersistentFallback = false;

    // Default to temp directory (ephemeral) - suitable for serverless
    // Respects TMPDIR/TMP/TEMP environment variables (common in serverless/Inngest)
    const systemTempDir = getSystemTempDir();
    baseDir = path.join(systemTempDir, "ocr-app");
    usePersistentFallback = true; // Enable fallback if temp validation fails

  // Validate and ensure the base directory exists
  try {
    baseDir = validateAndEnsureDirectory(baseDir);
  } catch (error) {
    // If validation fails and we're using temp directory, fallback to persistent
    if (usePersistentFallback) {
      console.warn(
        `Failed to use temp directory (${baseDir}), falling back to persistent application data directory.`,
        error instanceof Error ? error.message : String(error),
      );
      baseDir = getPersistentAppDataDir();
      baseDir = validateAndEnsureDirectory(baseDir);
    } else {
      // If OCR_BASE_DIR was explicitly set and validation fails, throw error
      throw new Error(
        `Failed to validate base volume directory. ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  // Add instance isolation to prevent multi-instance conflicts
  const instanceId = getInstanceId();
  const isolatedBaseDir = path.join(baseDir, "instances", instanceId);

  // Ensure the isolated instance directory exists
  fs.mkdirSync(isolatedBaseDir, { recursive: true });

  return isolatedBaseDir;
}

/**
 * Lazy initialization for serverless environments (e.g., Inngest).
 *
 * In serverless environments like Inngest:
 * - The filesystem is ephemeral and may not be available at module load time
 * - Each function execution may run in a different container/instance
 * - Temporary files are only needed during processing (download → process → upload to R2)
 * - Files are automatically cleaned up when the execution completes
 *
 * The base directory is computed on first access (when VOLUME_DIRS is first used),
 * not at module load time. This ensures compatibility with Inngest's execution model.
 */
let _BASE_VOLUME_DIR: string | null = null;

type VolumeDirs = {
  base: string;
  imagesBase: string;
  txtBase: string;
  wordBase: string;
  tmpBase: string;
};

let _VOLUME_DIRS_CACHE: VolumeDirs | null = null;

function getBaseVolumeDirLazy(): string {
  if (_BASE_VOLUME_DIR === null) {
    _BASE_VOLUME_DIR = getBaseVolumeDir();
  }
  return _BASE_VOLUME_DIR;
}

function getVolumeDirs(): VolumeDirs {
  if (_VOLUME_DIRS_CACHE === null) {
    const baseDir = getBaseVolumeDirLazy();
    _VOLUME_DIRS_CACHE = {
      base: baseDir,
      imagesBase: path.join(baseDir, "image-files"),
      txtBase: path.join(baseDir, "txt"),
      wordBase: path.join(baseDir, "word"),
      tmpBase: path.join(baseDir, "tmp"),
    };
  }
  return _VOLUME_DIRS_CACHE;
}

/**
 * Volume directories for OCR operations.
 *
 * Uses a Proxy to ensure lazy initialization in serverless environments like Inngest.
 * This prevents filesystem operations at module load time, which is critical because:
 * - Module loading happens before the function execution context is fully initialized
 * - The filesystem may not be available or writable at that point
 * - Each property access triggers initialization only when actually needed
 *
 * The Proxy implements enumeration support, so operations like Object.keys(),
 * Object.entries(), spread operator, and the `in` operator work as expected.
 *
 * @example
 * // First access initializes the directories
 * const imagesDir = VOLUME_DIRS.imagesBase; // Initialization happens here
 * const txtDir = VOLUME_DIRS.txtBase; // Uses cached value
 *
 * // Enumeration operations work correctly
 * const keys = Object.keys(VOLUME_DIRS); // ['base', 'imagesBase', 'txtBase', 'wordBase', 'tmpBase']
 * const entries = Object.entries(VOLUME_DIRS); // [['base', '...'], ...]
 * const spread = { ...VOLUME_DIRS }; // Creates a plain object with all properties
 * const hasBase = 'base' in VOLUME_DIRS; // true
 */
export const VOLUME_DIRS = new Proxy({} as VolumeDirs, {
  get(_target, prop) {
    const dirs = getVolumeDirs();
    return dirs[prop as keyof VolumeDirs];
  },
  ownKeys(_target) {
    const dirs = getVolumeDirs();
    return Reflect.ownKeys(dirs);
  },
  getOwnPropertyDescriptor(_target, prop) {
    const dirs = getVolumeDirs();
    if (prop in dirs) {
      return {
        enumerable: true,
        configurable: true,
        value: dirs[prop as keyof VolumeDirs],
      };
    }
    return undefined;
  },
  has(_target, prop) {
    const dirs = getVolumeDirs();
    return prop in dirs;
  },
});

export function getJobRootDir(jobId: string) {
  return path.join(VOLUME_DIRS.imagesBase, jobId);
}

export function getJobZipPath(jobId: string) {
  return path.join(getJobRootDir(jobId), "input.zip");
}

export function getJobRawDir(jobId: string) {
  return path.join(getJobRootDir(jobId), "raw");
}

export function getJobNormalizedDir(jobId: string) {
  return path.join(getJobRootDir(jobId), "normalized");
}

export function getJobCropsDir(jobId: string) {
  return path.join(getJobRootDir(jobId), "crops");
}

export function getJobTxtPath(jobId: string) {
  return path.join(VOLUME_DIRS.txtBase, `${jobId}.txt`);
}

export function getJobDocxPath(jobId: string) {
  return path.join(VOLUME_DIRS.wordBase, `${jobId}.docx`);
}

export function getJobBatchJsonlPath(jobId: string) {
  return path.join(VOLUME_DIRS.tmpBase, `${jobId}-ocr-batch.jsonl`);
}

export function getJobRawArchivePath(jobId: string) {
  return path.join(getJobRootDir(jobId), "raw-images.zip");
}