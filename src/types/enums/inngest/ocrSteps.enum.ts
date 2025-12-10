export const enum OcrStepId {
  BuildRawZip = "ocr.build-raw-zip",
  PreprocessImagesAndCrops = "ocr.preprocess-images-and-crops",
  ProcessImageBatch = "ocr.process-image-batch",
  CreateAndAwaitBatch = "ocr.create-and-await-batch",
  SaveResultsToDb = "ocr.save-results-to-db",
  BuildDocsAndCleanup = "ocr.build-docs-from-db-and-cleanup",
}