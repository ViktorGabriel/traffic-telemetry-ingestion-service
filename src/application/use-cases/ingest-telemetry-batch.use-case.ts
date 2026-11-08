import { TelemetryBatchSchema } from "../../domain/telemetry/telemetry-event.schema.js";
import { TelemetryPublisherPort } from "../../domain/telemetry/telemetry-publisher.port.js";

export interface IngestBatchOutput {
  success: boolean;
  published_count: number;
  stream_name: string;
  duration_ms: number;
}

export class IngestTelemetryBatchUseCase {
  constructor(private readonly publisher: TelemetryPublisherPort) {}

  /**
   * Executes batch validation and non-blocking stream publication.
   */
  async execute(input: unknown): Promise<IngestBatchOutput> {
    // 1. Fail-fast defensive validation via Zod
    const validatedEvents = TelemetryBatchSchema.parse(input);

    // 2. High-performance non-blocking publication
    const publishResult = await this.publisher.publishBatch(validatedEvents);

    return {
      success: true,
      published_count: publishResult.published_count,
      stream_name: publishResult.stream_name,
      duration_ms: publishResult.duration_ms
    };
  }
}
