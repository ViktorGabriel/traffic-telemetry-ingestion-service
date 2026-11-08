import { TelemetryPublisherPort, PublishResult } from "../../domain/telemetry/telemetry-publisher.port.js";
import { TelemetryBatch, TelemetryEvent } from "../../domain/telemetry/telemetry-event.schema.js";

export class InMemoryTelemetryPublisher implements TelemetryPublisherPort {
  public events: TelemetryEvent[] = [];
  public shouldFail: boolean = false;
  public failureError: Error = new Error("Simulated Redis failure");

  async publishBatch(events: TelemetryBatch): Promise<PublishResult> {
    if (this.shouldFail) {
      throw this.failureError;
    }

    const start = performance.now();
    this.events.push(...events);
    const duration_ms = Number((performance.now() - start).toFixed(2));

    return {
      published_count: events.length,
      stream_name: "test-traffic-telemetry-stream",
      duration_ms
    };
  }

  async publishSingle(event: TelemetryEvent): Promise<string> {
    if (this.shouldFail) {
      throw this.failureError;
    }

    this.events.push(event);
    return `msg-${this.events.length}`;
  }

  clear(): void {
    this.events = [];
    this.shouldFail = false;
  }
}
