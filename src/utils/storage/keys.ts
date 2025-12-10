const STORAGE_PREFIXES = {
  imagesBase: "image-files",
  txtBase: "txt",
  wordBase: "word",
  tmpBase: "tmp",
};

export const getJobRootKey = (jobId: string) =>
  `${STORAGE_PREFIXES.imagesBase}/${jobId}`;

export const getJobZipKey = (jobId: string) =>
  `${getJobRootKey(jobId)}/input.zip`;

export const getJobRawArchiveKey = (jobId: string) =>
  `${getJobRootKey(jobId)}/raw-images.zip`;

export const getJobTxtKey = (jobId: string) =>
  `${STORAGE_PREFIXES.txtBase}/${jobId}.txt`;

export const getJobDocxKey = (jobId: string) =>
  `${STORAGE_PREFIXES.wordBase}/${jobId}.docx`;

export const getJobBatchJsonlKey = (jobId: string) =>
  `${STORAGE_PREFIXES.tmpBase}/${jobId}-ocr-batch.jsonl`;

export const getJobThumbnailKey = (jobId: string) =>
  `${getJobRootKey(jobId)}/thumbnail.jpg`;

export const getJobCropKey = (jobId: string, filename: string) =>
  `${getJobRootKey(jobId)}/crops/${filename}`;

