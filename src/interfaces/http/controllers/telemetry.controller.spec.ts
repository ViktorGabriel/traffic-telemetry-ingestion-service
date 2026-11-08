import { describe, it, expect, beforeEach } from "vitest";
import { FastifyInstance } from "fastify";
import { buildApp } from "../../../app.js";
import { InMemoryTelemetryPublisher } from "../../../infrastructure/repositories/in-memory-telemetry.publisher.js";

describe("Telemetry HTTP Endpoints & Global Error Handling (Integration)", () => {
  let app: FastifyInstance;
  let publisher: InMemoryTelemetryPublisher;

  beforeEach(() => {
    publisher = new InMemoryTelemetryPublisher();
    app = buildApp({ publisher, logger: false });
  });

  it("POST /telemetry/events - should accept valid batch ingestion and return 202", async () => {
    const validBatch = [
      {
        device_id: "toll-camera-sp-280",
        timestamp: "2026-11-08T01:30:00.000Z",
        event_type: "SPEED_UPDATE",
        speed_kmh: 112.4,
        payload: {
          coordinates: {
            latitude: -23.532,
            longitude: -46.789,
            heading_deg: 95
          },
          metadata: { toll_gate: 3 }
        }
      },
      {
        device_id: "smart-sensor-radar-09",
        timestamp: "2026-11-08T01:30:01.000Z",
        event_type: "INCIDENT_ALERT",
        speed_kmh: 0,
        payload: {
          severity: "CRITICAL",
          metadata: { incident_type: "VEHICLE_STOPPED_IN_LANE" }
        }
      }
    ];

    const response = await app.inject({
      method: "POST",
      url: "/telemetry/events",
      payload: validBatch
    });

    expect(response.statusCode).toBe(202);
    const body = response.json();
    expect(body.status).toBe("ACCEPTED");
    expect(body.data.published_count).toBe(2);
    expect(publisher.events.length).toBe(2);
  });

  it("POST /telemetry/events - should return 400 when payload fails Zod validation", async () => {
    const invalidBatch = [
      {
        device_id: "ab", // Too short (min 3)
        timestamp: "invalid-date",
        event_type: "SPEED_UPDATE",
        speed_kmh: 400 // Exceeds max 350 km/h
      }
    ];

    const response = await app.inject({
      method: "POST",
      url: "/telemetry/events",
      payload: invalidBatch
    });

    expect(response.statusCode).toBe(400);
    const body = response.json();
    expect(body.error).toBe("Bad Request");
    expect(body.details).toBeInstanceOf(Array);
    expect(body.details.length).toBeGreaterThan(0);
    expect(publisher.events.length).toBe(0);
  });

  it("POST /telemetry/events - should return 503 when Redis stream publisher fails", async () => {
    publisher.shouldFail = true;
    publisher.failureError = new Error("Redis connection timeout: ETIMEDOUT");

    const validBatch = [
      {
        device_id: "sensor-test-01",
        timestamp: "2026-11-08T01:30:00.000Z",
        event_type: "SPEED_UPDATE",
        speed_kmh: 70
      }
    ];

    const response = await app.inject({
      method: "POST",
      url: "/telemetry/events",
      payload: validBatch
    });

    expect(response.statusCode).toBe(503);
    const body = response.json();
    expect(body.error).toBe("Service Unavailable");
    expect(body.message).toContain("Upstream backpressure active");
  });

  it("GET /health - should return 200 UP", async () => {
    const response = await app.inject({
      method: "GET",
      url: "/health"
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe("UP");
  });
});
