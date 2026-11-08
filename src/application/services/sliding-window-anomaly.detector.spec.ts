import { describe, it, expect, beforeEach } from "vitest";
import { SlidingWindowAnomalyDetector } from "./sliding-window-anomaly.detector.js";
import { TelemetryEvent } from "../../domain/telemetry/telemetry-event.schema.js";

describe("SlidingWindowAnomalyDetector (Concurrency & Algorithmic Unit Tests)", () => {
  let detector: SlidingWindowAnomalyDetector;

  beforeEach(() => {
    detector = new SlidingWindowAnomalyDetector({
      speedThresholdKmh: 100,
      consecutiveViolationsThreshold: 3,
      signalLossTimeoutMs: 60000 // 60 seconds
    });
  });

  const createEvent = (
    deviceId: string,
    speedKmh: number,
    timestamp: string,
    latitude = -23.55,
    longitude = -46.63
  ): TelemetryEvent => ({
    device_id: deviceId,
    timestamp,
    event_type: "SPEED_UPDATE",
    speed_kmh: speedKmh,
    payload: {
      coordinates: { latitude, longitude }
    }
  });

  describe("Consecutive Speeding Detection", () => {
    it("should detect an incident after 3 consecutive speeding readings", () => {
      const now = new Date("2026-11-08T00:00:00.000Z").getTime();

      // Reading 1: over limit (110 km/h)
      const inc1 = detector.processEvent(
        createEvent("truck-01", 110, new Date(now).toISOString())
      );
      expect(inc1).toBeNull();

      // Reading 2: over limit (115 km/h)
      const inc2 = detector.processEvent(
        createEvent("truck-01", 115, new Date(now + 1000).toISOString())
      );
      expect(inc2).toBeNull();

      // Reading 3: over limit (125 km/h) -> Should trigger CONSECUTIVE_SPEEDING incident!
      const inc3 = detector.processEvent(
        createEvent("truck-01", 125, new Date(now + 2000).toISOString())
      );

      expect(inc3).not.toBeNull();
      expect(inc3?.incident_type).toBe("CONSECUTIVE_SPEEDING");
      expect(inc3?.device_id).toBe("truck-01");
      expect(inc3?.details.consecutive_violations).toBe(3);
      expect(inc3?.details.speed_kmh).toBe(125);
      expect(inc3?.details.speed_readings).toEqual([110, 115, 125]);
    });

    it("should reset the consecutive counter when a normal speed reading arrives", () => {
      const now = new Date("2026-11-08T00:00:00.000Z").getTime();

      // 2 violations
      detector.processEvent(createEvent("car-99", 105, new Date(now).toISOString()));
      detector.processEvent(createEvent("car-99", 110, new Date(now + 1000).toISOString()));

      // Normal speed: 80 km/h -> Streak broken
      const incNormal = detector.processEvent(
        createEvent("car-99", 80, new Date(now + 2000).toISOString())
      );
      expect(incNormal).toBeNull();

      // Next speeding reading is count = 1, should not trigger
      const incAfter = detector.processEvent(
        createEvent("car-99", 115, new Date(now + 3000).toISOString())
      );
      expect(incAfter).toBeNull();
    });

    it("should classify high speeding (>140 km/h) with CRITICAL severity", () => {
      const now = new Date("2026-11-08T00:00:00.000Z").getTime();

      detector.processEvent(createEvent("moto-07", 145, new Date(now).toISOString()));
      detector.processEvent(createEvent("moto-07", 150, new Date(now + 1000).toISOString()));
      const incident = detector.processEvent(
        createEvent("moto-07", 155, new Date(now + 2000).toISOString())
      );

      expect(incident).not.toBeNull();
      expect(incident?.severity).toBe("CRITICAL");
    });
  });

  describe("Signal Loss Detection (> 60 seconds)", () => {
    it("should detect signal loss when a device stops emitting for more than 60s", () => {
      const t0 = new Date("2026-11-08T00:00:00.000Z").getTime();

      // Device emits event at t0
      detector.processEvent(createEvent("bus-404", 45, new Date(t0).toISOString()));

      // Check after 30 seconds -> No signal loss yet
      const incidents30s = detector.checkSignalLoss(t0 + 30000);
      expect(incidents30s.length).toBe(0);

      // Check after 61 seconds -> Signal loss triggered!
      const incidents61s = detector.checkSignalLoss(t0 + 61000);
      expect(incidents61s.length).toBe(1);
      expect(incidents61s[0].incident_type).toBe("SIGNAL_LOSS");
      expect(incidents61s[0].device_id).toBe("bus-404");
      expect(incidents61s[0].severity).toBe("HIGH");
      expect(incidents61s[0].details.elapsed_seconds_without_signal).toBeGreaterThanOrEqual(60);
    });

    it("should not re-trigger signal loss repeatedly on every check unless device recovers first", () => {
      const t0 = new Date("2026-11-08T00:00:00.000Z").getTime();

      detector.processEvent(createEvent("bus-404", 45, new Date(t0).toISOString()));

      // First check at 65s -> 1 alert
      const firstCheck = detector.checkSignalLoss(t0 + 65000);
      expect(firstCheck.length).toBe(1);

      // Second check at 80s -> 0 alerts (avoid spamming ops dashboard)
      const secondCheck = detector.checkSignalLoss(t0 + 80000);
      expect(secondCheck.length).toBe(0);

      // Device sends signal again at 90s (reconnects)
      detector.processEvent(createEvent("bus-404", 40, new Date(t0 + 90000).toISOString()));

      // If it disappears again for another 61s (at 152s) -> New alert
      const thirdCheck = detector.checkSignalLoss(t0 + 152000);
      expect(thirdCheck.length).toBe(1);
      expect(thirdCheck[0].incident_type).toBe("SIGNAL_LOSS");
    });
  });
});
