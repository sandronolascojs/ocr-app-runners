import { serve } from 'inngest/fastify';
import Fastify from 'fastify';
import { inngest } from './inngest/client';
import { processOcrJob } from './inngest/functions/processOcrJob';
import { env } from './config/env.config';

const fastify = Fastify({
  logger: true,
});

const port = env.PORT;

// Inngest serve endpoint
fastify.route({
  method: ['GET', 'POST', 'PUT'],
  handler: serve({ client: inngest, functions: [processOcrJob] }),
  url: '/api/inngest/*',
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

