import { TelemetryRepositoryPort } from "../../domain/telemetry/telemetry-repository.port.js";
import { TelemetryBatch, TelemetryEvent } from "../../domain/telemetry/telemetry-event.schema.js";

export class InMemoryTelemetryRepository implements TelemetryRepositoryPort {
  public storedEvents: TelemetryEvent[] = [];

  async saveBatch(events: TelemetryBatch): Promise<number> {
    this.storedEvents.push(...events);
    return events.length;
  }

  clear(): void {
    this.storedEvents = [];
  }
}
