import Fastify, { FastifyRequest, FastifyReply } from 'fastify';
import { StateDiffAnalyzer } from './StateDiffAnalyzer';
import { EventAggregator } from './EventAggregator';
import { JupiterEnricher } from './plugins/JupiterEnricher';

const VERSION = '1.0.0';
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;

const fastify = Fastify({ logger: true });

interface DecodeRequestBody {
  raw_tx: any;
}

interface DecodeSuccessResponse {
  event: any;
  version: string;
}

interface DecodeErrorResponse {
  error: string;
  message: string;
}

fastify.post(
  '/decode',
  async (
    request: FastifyRequest<{ Body: DecodeRequestBody }>,
    reply: FastifyReply
  ) => {
    fastify.log.info('Received /decode request');
    try {
      const { raw_tx } = request.body;
      if (!raw_tx) {
        fastify.log.info('Missing raw_tx in request body');
        return reply.status(400).send({
          error: 'INVALID_REQUEST',
          message: 'Missing required field: raw_tx',
        } as DecodeErrorResponse);
      }
      // 1. StateDiffAnalyzer
      const analyzer = new StateDiffAnalyzer();
      const atomicEvents = analyzer.analyze(raw_tx);
      // 2. EventAggregator + JupiterEnricher
      const aggregator = new EventAggregator({}, [new JupiterEnricher()]);
      const event = await aggregator.aggregate(atomicEvents, raw_tx);
      // 3. Ответ
      fastify.log.info('Successfully processed /decode request');
      return reply.status(200).send({ event, version: VERSION } as DecodeSuccessResponse);
    } catch (err: any) {
      fastify.log.error({ err }, 'Error in /decode');
      return reply.status(500).send({
        error: 'DECODING_FAILED',
        message: err?.message || 'Unknown error during decoding.',
      } as DecodeErrorResponse);
    }
  }
);

fastify.listen({ port: PORT, host: '0.0.0.0' }, (err, address) => {
  if (err) {
    fastify.log.error(err, 'Failed to start server');
    process.exit(1);
  }
  fastify.log.info(`Decoder service listening on ${address}`);
}); 