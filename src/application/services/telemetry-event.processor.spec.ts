import { describe, it, expect, beforeEach } from "vitest";
import { TelemetryEventProcessor } from "./telemetry-event.processor.js";
import { SlidingWindowAnomalyDetector } from "./sliding-window-anomaly.detector.js";
import { InMemoryTelemetryConsumer } from "../../infrastructure/repositories/in-memory-telemetry.consumer.js";
import { InMemoryIncidentBroadcaster } from "../../infrastructure/repositories/in-memory-incident.broadcaster.js";

describe("TelemetryEventProcessor (Stream Consumer & WebSocket Dispatcher)", () => {
  let consumer: InMemoryTelemetryConsumer;
  let broadcaster: InMemoryIncidentBroadcaster;
  let detector: SlidingWindowAnomalyDetector;
  let processor: TelemetryEventProcessor;

  beforeEach(() => {
    consumer = new InMemoryTelemetryConsumer();
    broadcaster = new InMemoryIncidentBroadcaster();
    detector = new SlidingWindowAnomalyDetector({
      speedThresholdKmh: 100,
      consecutiveViolationsThreshold: 3,
      signalLossTimeoutMs: 60000
    });

    processor = new TelemetryEventProcessor({
      consumer,
      broadcaster,
      detector
    });
  });

  it("should consume batch, detect consecutive speeding, broadcast via WebSocket and acknowledge stream IDs", async () => {
    const t0 = new Date("2026-11-08T00:00:00.000Z").getTime();

    // Enqueue 3 stream messages exceeding 100 km/h
    consumer.addMessage({
      id: "1699999999000-0",
      event: {
        device_id: "vehicle-alpha",
        timestamp: new Date(t0).toISOString(),
        event_type: "SPEED_UPDATE",
        speed_kmh: 110,
        payload: { coordinates: { latitude: -23.5, longitude: -46.6 } }
      }
    });

    consumer.addMessage({
      id: "1699999999000-1",
      event: {
        device_id: "vehicle-alpha",
        timestamp: new Date(t0 + 1000).toISOString(),
        event_type: "SPEED_UPDATE",
        speed_kmh: 118,
        payload: { coordinates: { latitude: -23.51, longitude: -46.61 } }
      }
    });

    consumer.addMessage({
      id: "1699999999000-2",
      event: {
        device_id: "vehicle-alpha",
        timestamp: new Date(t0 + 2000).toISOString(),
        event_type: "SPEED_UPDATE",
        speed_kmh: 130,
        payload: { coordinates: { latitude: -23.52, longitude: -46.62 } }
      }
    });

    // Execute one iteration of consumer poll
    const processedCount = await processor.processNextBatch();

    expect(processedCount).toBe(3);
    // Messages must be acknowledged
    expect(consumer.acknowledgedIds).toEqual([
      "1699999999000-0",
      "1699999999000-1",
      "1699999999000-2"
    ]);

    // WebSocket broadcaster must have received exactly 1 incident
    expect(broadcaster.broadcastedIncidents.length).toBe(1);
    const incident = broadcaster.broadcastedIncidents[0];
    expect(incident.incident_type).toBe("CONSECUTIVE_SPEEDING");
    expect(incident.device_id).toBe("vehicle-alpha");
    expect(incident.severity).toBe("HIGH");
    expect(incident.details.consecutive_violations).toBe(3);
  });

  it("should check and broadcast signal loss incidents when devices are silent > 60s", async () => {
    const t0 = new Date("2026-11-08T00:00:00.000Z").getTime();

    // Vehicle sends telemetry at t0
    consumer.addMessage({
      id: "1699999999000-3",
      event: {
        device_id: "patrol-car-10",
        timestamp: new Date(t0).toISOString(),
        event_type: "SPEED_UPDATE",
        speed_kmh: 60,
        payload: {}
      }
    });

    await processor.processNextBatch();
    expect(broadcaster.broadcastedIncidents.length).toBe(0);

    // Trigger signal loss check with 65s elapsed
    const incidents = await processor.checkSignalLoss(t0 + 65000);

    expect(incidents.length).toBe(1);
    expect(incidents[0].incident_type).toBe("SIGNAL_LOSS");
    expect(incidents[0].device_id).toBe("patrol-car-10");
    expect(broadcaster.broadcastedIncidents.length).toBe(1);
    expect(broadcaster.broadcastedIncidents[0].incident_id).toBe(incidents[0].incident_id);
  });
});
