import { randomUUID } from "node:crypto";
import { TelemetryEvent, GeoCoordinates } from "../../domain/telemetry/telemetry-event.schema.js";
import { IncidentDetected, IncidentSeverity } from "../../domain/incidents/incident.schema.js";

export interface SlidingWindowDetectorConfig {
  speedThresholdKmh?: number;
  consecutiveViolationsThreshold?: number;
  signalLossTimeoutMs?: number;
}

interface DeviceState {
  lastSeenTimestampMs: number;
  lastIsoTimestamp: string;
  lastCoordinates?: GeoCoordinates;
  consecutiveSpeedingViolations: number;
  recentSpeedReadings: number[];
  signalLossAlerted: boolean;
}

export class SlidingWindowAnomalyDetector {
  private readonly speedThresholdKmh: number;
  private readonly consecutiveViolationsThreshold: number;
  private readonly signalLossTimeoutMs: number;

  // In-memory sliding state keyed by device_id
  private readonly deviceStates = new Map<string, DeviceState>();

  constructor(config: SlidingWindowDetectorConfig = {}) {
    this.speedThresholdKmh = config.speedThresholdKmh ?? 100;
    this.consecutiveViolationsThreshold = config.consecutiveViolationsThreshold ?? 3;
    this.signalLossTimeoutMs = config.signalLossTimeoutMs ?? 60_000; // 60 seconds
  }

  /**
   * Evaluates an incoming telemetry event against the device's sliding window.
   * Returns an IncidentDetected if consecutive speeding is confirmed, or null otherwise.
   */
  processEvent(event: TelemetryEvent): IncidentDetected | null {
    const eventTimeMs = new Date(event.timestamp).getTime();
    let state = this.deviceStates.get(event.device_id);

    if (!state) {
      state = {
        lastSeenTimestampMs: eventTimeMs,
        lastIsoTimestamp: event.timestamp,
        lastCoordinates: event.payload.coordinates,
        consecutiveSpeedingViolations: 0,
        recentSpeedReadings: [],
        signalLossAlerted: false
      };
      this.deviceStates.set(event.device_id, state);
    } else {
      // Update last seen
      if (eventTimeMs > state.lastSeenTimestampMs) {
        state.lastSeenTimestampMs = eventTimeMs;
        state.lastIsoTimestamp = event.timestamp;
      }
      if (event.payload.coordinates) {
        state.lastCoordinates = event.payload.coordinates;
      }
      // If device had lost signal earlier, it has now recovered
      if (state.signalLossAlerted) {
        state.signalLossAlerted = false;
      }
    }

    // 1. Sliding window consecutive speeding detection
    if (event.speed_kmh > this.speedThresholdKmh) {
      state.consecutiveSpeedingViolations += 1;
      state.recentSpeedReadings.push(event.speed_kmh);

      // Keep only sliding window size
      if (state.recentSpeedReadings.length > this.consecutiveViolationsThreshold) {
        state.recentSpeedReadings.shift();
      }

      // Check if threshold reached
      if (state.consecutiveSpeedingViolations >= this.consecutiveViolationsThreshold) {
        const severity: IncidentSeverity =
          event.speed_kmh >= 140
            ? "CRITICAL"
            : event.speed_kmh >= 120
              ? "HIGH"
              : "MEDIUM";

        const incident: IncidentDetected = {
          incident_id: randomUUID(),
          device_id: event.device_id,
          incident_type: "CONSECUTIVE_SPEEDING",
          severity,
          detected_at: new Date().toISOString(),
          details: {
            description: `Device ${event.device_id} exceeded speed limit (${this.speedThresholdKmh} km/h) for ${state.consecutiveSpeedingViolations} consecutive readings.`,
            speed_kmh: event.speed_kmh,
            speed_limit_kmh: this.speedThresholdKmh,
            consecutive_violations: state.consecutiveSpeedingViolations,
            speed_readings: [...state.recentSpeedReadings],
            coordinates: state.lastCoordinates
          }
        };

        // Reset streak after emitting incident to prevent alert storm
        state.consecutiveSpeedingViolations = 0;
        state.recentSpeedReadings = [];

        return incident;
      }
    } else {
      // Normal speed breaks consecutive speeding streak
      state.consecutiveSpeedingViolations = 0;
      state.recentSpeedReadings = [];
    }

    return null;
  }

  /**
   * Sweeps known devices and detects signal loss (> 60 seconds without events).
   */
  checkSignalLoss(currentTimeMs: number = Date.now()): IncidentDetected[] {
    const detectedIncidents: IncidentDetected[] = [];

    for (const [deviceId, state] of this.deviceStates.entries()) {
      const elapsedMs = currentTimeMs - state.lastSeenTimestampMs;

      if (!state.signalLossAlerted && elapsedMs >= this.signalLossTimeoutMs) {
        state.signalLossAlerted = true;

        const elapsedSeconds = Math.floor(elapsedMs / 1000);

        const incident: IncidentDetected = {
          incident_id: randomUUID(),
          device_id: deviceId,
          incident_type: "SIGNAL_LOSS",
          severity: "HIGH",
          detected_at: new Date().toISOString(),
          details: {
            description: `Telemetry signal lost for device ${deviceId}. Inactive for ${elapsedSeconds}s (threshold: ${this.signalLossTimeoutMs / 1000}s).`,
            last_seen_at: state.lastIsoTimestamp,
            elapsed_seconds_without_signal: elapsedSeconds,
            coordinates: state.lastCoordinates
          }
        };

        detectedIncidents.push(incident);
      }
    }

    return detectedIncidents;
  }

  getMonitoredDeviceCount(): number {
    return this.deviceStates.size;
  }

  clear(): void {
    this.deviceStates.clear();
  }
}
