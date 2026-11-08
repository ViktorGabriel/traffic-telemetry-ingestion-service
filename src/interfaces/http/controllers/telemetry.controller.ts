import { FastifyReply, FastifyRequest } from "fastify";
import { IngestTelemetryBatchUseCase } from "../../../application/use-cases/ingest-telemetry-batch.use-case.js";

export class TelemetryController {
  constructor(private readonly ingestBatchUseCase: IngestTelemetryBatchUseCase) {}

  /**
   * POST /telemetry/events
   * High-throughput batch ingestion endpoint.
   */
  async ingestBatch(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    const result = await this.ingestBatchUseCase.execute(request.body);

    reply.status(202).send({
      statusCode: 202,
      status: "ACCEPTED",
      message: "Telemetry batch ingested and queued for stream processing",
      data: {
        published_count: result.published_count,
        stream_name: result.stream_name,
        duration_ms: result.duration_ms
      }
    });
  }
}
