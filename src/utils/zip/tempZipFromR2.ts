import * as fs from "node:fs/promises";

import { env } from "@/config/env.config";
import { downloadObjectToTempFileVerified } from "@/utils/storage";

export type TempZipContext = {
  zipKey: string;
  tempPath: string;
  sizeBytes: number;
};

export const withTempZipFromR2 = async <T>(params: {
  zipKey: string;
  prefix: string;
  fn: (ctx: TempZipContext) => Promise<T>;
}): Promise<T> => {
  const { tempPath, sizeBytes, release } = await downloadObjectToTempFileVerified({
    key: params.zipKey,
    prefix: params.prefix,
  });

  let ok = false;
  try {
    const result = await params.fn({ zipKey: params.zipKey, tempPath, sizeBytes });
    ok = true;
    return result;
  } finally {
    if (!ok && env.R2_SPOOL_KEEP_FILES_ON_ERROR) {
      console.warn(`[zip-spool] keeping temp zip for inspection: ${tempPath}`);
    } else {
      await fs.unlink(tempPath).catch(() => undefined);
    }
    release();
  }
};


