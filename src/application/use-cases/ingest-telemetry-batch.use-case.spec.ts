import { describe, it, expect, beforeEach } from "vitest";
import { IngestTelemetryBatchUseCase } from "./ingest-telemetry-batch.use-case.js";
import { InMemoryTelemetryPublisher } from "../../infrastructure/repositories/in-memory-telemetry.publisher.js";
import { ZodError } from "zod";

describe("IngestTelemetryBatchUseCase (Concurrency & Validation Unit Tests)", () => {
  let publisher: InMemoryTelemetryPublisher;
  let useCase: IngestTelemetryBatchUseCase;

  beforeEach(() => {
    publisher = new InMemoryTelemetryPublisher();
    useCase = new IngestTelemetryBatchUseCase(publisher);
  });

  it("should validate and ingest a batch of valid traffic telemetry events", async () => {
    const rawBatch = [
      {
        device_id: "sensor-br-101-km42",
        timestamp: new Date().toISOString(),
        event_type: "SPEED_UPDATE",
        speed_kmh: 88.5,
        payload: {
          coordinates: {
            latitude: -23.55052,
            longitude: -46.633308,
            heading_deg: 180
          },
          metadata: { lane: 1, weather: "rain" }
        }
      },
      {
        device_id: "cam-sp-015",
        timestamp: new Date().toISOString(),
        event_type: "INCIDENT_ALERT",
        speed_kmh: 0,
        payload: {
          severity: "HIGH",
          coordinates: {
            latitude: -23.551,
            longitude: -46.634
          }
        }
      }
    ];

    const result = await useCase.execute(rawBatch);

    expect(result.success).toBe(true);
    expect(result.published_count).toBe(2);
    expect(publisher.events.length).toBe(2);
    expect(publisher.events[0].device_id).toBe("sensor-br-101-km42");
  });

  it("should fail-fast if the batch is empty", async () => {
    const emptyBatch: unknown[] = [];

    await expect(useCase.execute(emptyBatch)).rejects.toThrow(ZodError);
    expect(publisher.events.length).toBe(0);
  });

  it("should reject batch with invalid speed (e.g., negative speed or unrealistic > 350 km/h)", async () => {
    const invalidBatch = [
      {
        device_id: "radar-01",
        timestamp: new Date().toISOString(),
        event_type: "SPEED_UPDATE",
        speed_kmh: -15, // Invalid
        payload: {}
      }
    ];

    await expect(useCase.execute(invalidBatch)).rejects.toThrow(ZodError);
  });

  it("should reject batch with invalid timestamp", async () => {
    const invalidBatch = [
      {
        device_id: "radar-01",
        timestamp: "not-a-timestamp",
        event_type: "SPEED_UPDATE",
        speed_kmh: 60,
        payload: {}
      }
    ];

    await expect(useCase.execute(invalidBatch)).rejects.toThrow(ZodError);
  });

  it("should propagate infrastructure errors when publisher stream fails", async () => {
    publisher.shouldFail = true;

    const validBatch = [
      {
        device_id: "sensor-01",
        timestamp: new Date().toISOString(),
        event_type: "SPEED_UPDATE",
        speed_kmh: 60,
        payload: {}
      }
    ];

    await expect(useCase.execute(validBatch)).rejects.toThrow("Simulated Redis failure");
  });
});
