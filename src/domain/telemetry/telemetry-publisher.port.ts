import { TelemetryBatch, TelemetryEvent } from "./telemetry-event.schema.js";

export interface PublishResult {
  published_count: number;
  stream_name: string;
  duration_ms: number;
}

export interface TelemetryPublisherPort {
  /**
   * Publishes a batch of telemetry events in a non-blocking pipelined manner.
   */
  publishBatch(events: TelemetryBatch): Promise<PublishResult>;

  /**
   * Publishes a single telemetry event.
   */
  publishSingle(event: TelemetryEvent): Promise<string>;
}
