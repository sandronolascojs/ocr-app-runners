import express from 'express';
import { serve } from 'inngest/express';
import { inngest } from './inngest/client';
import { env } from './config/env.config';
import { processOcrJob } from './inngest/functions/processOcrJob';
import { removeSubtitlesFromImages } from './inngest/functions/removeSubtitles';
import { cleanupOldJobFiles } from './inngest/functions/cleanupOldJobFiles';

const app = express();
const port = env.PORT;

app.use(express.json({ limit: '10mb' }));

app.use('/api/inngest', serve({
  client: inngest,
  functions: [
    processOcrJob,
    removeSubtitlesFromImages,
    cleanupOldJobFiles,
  ],
  streaming: true,
}));

app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Inngest server running on port ${port}`);
  console.log(`Inngest endpoint: http://localhost:${port}/api/inngest`);
});

