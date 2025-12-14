export const getUserRootKey = (userId: string) => `users/${userId}`;

export const getJobRootKey = (userId: string, jobId: string) =>
  `${getUserRootKey(userId)}/${jobId}`;

export const getJobZipKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/input.zip`;

export const getJobRawArchiveKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/raw-images.zip`;

export const getJobTxtKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/document.txt`;

export const getJobDocxKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/document.docx`;

export const getJobBatchJsonlKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/ocr-batch.jsonl`;

export const getJobThumbnailKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/thumbnail.jpg`;

export const getJobCropKey = (userId: string, jobId: string, filename: string) =>
  `${getJobRootKey(userId, jobId)}/crops/${filename}`;

export const getJobCroppedZipKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/cropped-images.zip`;

export const getJobCroppedThumbnailKey = (userId: string, jobId: string) =>
  `${getJobRootKey(userId, jobId)}/cropped-thumbnail.jpg`;

// Helper to get normalized image key (used during processing)
export const getJobNormalizedImageKey = (userId: string, jobId: string, filename: string) =>
  `${getJobRootKey(userId, jobId)}/normalized/${filename}`;

