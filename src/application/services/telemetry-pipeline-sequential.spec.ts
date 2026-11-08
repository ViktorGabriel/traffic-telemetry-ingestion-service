import { describe, it, expect, beforeEach } from "vitest";
import { TelemetryEventProcessor } from "./telemetry-event.processor.js";
import { SlidingWindowAnomalyDetector } from "./sliding-window-anomaly.detector.js";
import { InMemoryTelemetryConsumer } from "../../infrastructure/repositories/in-memory-telemetry.consumer.js";
import { InMemoryIncidentBroadcaster } from "../../infrastructure/repositories/in-memory-incident.broadcaster.js";
import { InMemoryTelemetryRepository } from "../../infrastructure/repositories/in-memory-telemetry.repository.js";
import { InMemoryIncidentRepository } from "../../infrastructure/repositories/in-memory-incident.repository.js";
import { StreamMessage } from "../../domain/telemetry/telemetry-consumer.port.js";

describe("Sequential 50-Event Telemetry Pipeline & Incident Verification Suite", () => {
  let consumer: InMemoryTelemetryConsumer;
  let broadcaster: InMemoryIncidentBroadcaster;
  let telemetryRepo: InMemoryTelemetryRepository;
  let incidentRepo: InMemoryIncidentRepository;
  let detector: SlidingWindowAnomalyDetector;
  let processor: TelemetryEventProcessor;

  beforeEach(() => {
    consumer = new InMemoryTelemetryConsumer();
    broadcaster = new InMemoryIncidentBroadcaster();
    telemetryRepo = new InMemoryTelemetryRepository();
    incidentRepo = new InMemoryIncidentRepository();

    detector = new SlidingWindowAnomalyDetector({
      speedThresholdKmh: 100,
      consecutiveViolationsThreshold: 3,
      signalLossTimeoutMs: 60000 // 60s
    });

    processor = new TelemetryEventProcessor({
      consumer,
      broadcaster,
      detector,
      telemetryRepository: telemetryRepo,
      incidentRepository: incidentRepo,
      batchSize: 50,
      blockMs: 100
    });
  });

  it("should process 50 sequential events, persist all to DB, and detect both consecutive speeding and signal loss", async () => {
    const baseTimestamp = new Date("2026-11-08T12:00:00.000Z").getTime();
    const fiftyMessages: StreamMessage[] = [];

    // 1. Generate 50 realistic sequential telemetry events
    for (let i = 0; i < 50; i++) {
      // Step each event by 1.5 seconds (total timeline: 75 seconds)
      const currentEventTime = baseTimestamp + i * 1500;
      const isoTime = new Date(currentEventTime).toISOString();
      const messageId = `1799000000000-${i}`;

      let deviceId: string;
      let speedKmh: number;

      if (i === 1) {
        // Silent vehicle: transmits once at i=1, then goes completely offline
        deviceId = "device-silent-ghost-01";
        speedKmh = 55.0;
      } else if (i >= 10 && i <= 12) {
        // Offending vehicle: 3 consecutive speeding readings (i=10, 11, 12)
        deviceId = "device-speeding-violator-99";
        // 122 km/h, 134 km/h, 145 km/h -> triggers CRITICAL incident
        speedKmh = 120 + (i - 9) * 8;
      } else if (i === 9 || i === 13) {
        // Normal speed for offending vehicle before and after
        deviceId = "device-speeding-violator-99";
        speedKmh = 72.0;
      } else {
        // Normal traffic fleet: device-fleet-A, device-fleet-B, device-fleet-C
        const fleetId = (i % 3) + 1;
        deviceId = `device-fleet-0${fleetId}`;
        speedKmh = 50 + (i % 30); // Speeds between 50 and 80 km/h (safe)
      }

      fiftyMessages.push({
        id: messageId,
        event: {
          device_id: deviceId,
          timestamp: isoTime,
          event_type: "SPEED_UPDATE",
          speed_kmh: speedKmh,
          payload: {
            coordinates: {
              latitude: -23.5505 + (i * 0.001),
              longitude: -46.6333 + (i * 0.001),
              heading_deg: 90
            },
            metadata: { sequence_seq: i }
          }
        }
      });
    }

    // Load all 50 sequential messages into stream queue
    for (const msg of fiftyMessages) {
      consumer.addMessage(msg);
    }

    expect(consumer.queue.length).toBe(50);

    // 2. Execute Batch Ingestion / Consumer processing
    const processedCount = await processor.processNextBatch();

    // Verify batch execution & stream acknowledgment
    expect(processedCount).toBe(50);
    expect(consumer.acknowledgedIds.length).toBe(50);
    expect(consumer.acknowledgedIds[0]).toBe("1799000000000-0");
    expect(consumer.acknowledgedIds[49]).toBe("1799000000000-49");

    // 3. Verify PostgreSQL batch persistence
    expect(telemetryRepo.storedEvents.length).toBe(50);
    expect(telemetryRepo.storedEvents[0].device_id).toBe("device-fleet-01");

    // 4. Verify Anomaly Detection (1): Consecutive Speeding Incident
    expect(broadcaster.broadcastedIncidents.length).toBe(1);
    const speedIncident = broadcaster.broadcastedIncidents[0];

    expect(speedIncident.incident_type).toBe("CONSECUTIVE_SPEEDING");
    expect(speedIncident.device_id).toBe("device-speeding-violator-99");
    expect(speedIncident.severity).toBe("CRITICAL"); // >= 140 km/h
    expect(speedIncident.details.consecutive_violations).toBe(3);
    expect(speedIncident.details.speed_readings).toEqual([128, 136, 144]);
    expect(incidentRepo.storedIncidents.length).toBe(1);
    expect(incidentRepo.storedIncidents[0].incident_id).toBe(speedIncident.incident_id);

    // 5. Verify Anomaly Detection (2): Signal Loss (>60s)
    // The 50 events covered 75 seconds. device-silent-ghost-01 has been silent for 73.5 seconds.
    const timelineEndTime = baseTimestamp + (49 * 1500); // 73,500ms from start
    const signalLossIncidents = await processor.checkSignalLoss(timelineEndTime);

    expect(signalLossIncidents.length).toBe(1);
    const signalIncident = signalLossIncidents[0];

    expect(signalIncident.incident_type).toBe("SIGNAL_LOSS");
    expect(signalIncident.device_id).toBe("device-silent-ghost-01");
    expect(signalIncident.severity).toBe("HIGH");
    expect(signalIncident.details.elapsed_seconds_without_signal).toBeGreaterThanOrEqual(60);

    // Broadcaster and Incident Repository must now hold both incidents
    expect(broadcaster.broadcastedIncidents.length).toBe(2);
    expect(incidentRepo.storedIncidents.length).toBe(2);
  });
});
