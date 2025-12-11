import { serve } from 'inngest/fastify';
import Fastify from 'fastify';
import { inngest } from './inngest/client';
import { preprocessZip } from './inngest/functions/preprocessZip';
import { createBatch } from './inngest/functions/createBatch';
import { waitForBatch } from './inngest/functions/waitForBatch';
import { processAllBatches } from './inngest/functions/processAllBatches';
import { buildDocuments } from './inngest/functions/buildDocuments';
import { env } from './config/env.config';

const fastify = Fastify({
  logger: true,
});

const port = env.PORT;

// Inngest serve endpoint
fastify.route({
  method: ['GET', 'POST', 'PUT'],
  handler: serve({
    client: inngest,
    functions: [
      preprocessZip,
      createBatch,
      waitForBatch,
      processAllBatches,
      buildDocuments,
    ],
  }),
  url: '/api/inngest',
});

// Health check endpoint
fastify.get('/health', async (_request, reply) => {
  return reply.status(200).send({ status: 'ok' });
});

const start = async () => {
  try {
    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`Inngest server running on port ${port}`);
    console.log(`Inngest endpoint: http://localhost:${port}/api/inngest`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();

