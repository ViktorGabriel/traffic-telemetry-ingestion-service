import { TelemetryBatch } from "./telemetry-event.schema.js";

export interface TelemetryRepositoryPort {
  /**
   * Persists a batch of telemetry events in PostgreSQL in a single multi-row operation.
   * Returns the count of persisted events.
   */
  saveBatch(events: TelemetryBatch): Promise<number>;
}
