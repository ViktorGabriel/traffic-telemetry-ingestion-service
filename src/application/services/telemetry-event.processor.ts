import { TelemetryConsumerPort } from "../../domain/telemetry/telemetry-consumer.port.js";
import { IncidentBroadcasterPort } from "../../domain/incidents/incident-broadcaster.port.js";
import { SlidingWindowAnomalyDetector } from "./sliding-window-anomaly.detector.js";
import { IncidentDetected } from "../../domain/incidents/incident.schema.js";
import { TelemetryRepositoryPort } from "../../domain/telemetry/telemetry-repository.port.js";
import { IncidentRepositoryPort } from "../../domain/incidents/incident-repository.port.js";
import { TelemetryEvent } from "../../domain/telemetry/telemetry-event.schema.js";

export interface TelemetryEventProcessorConfig {
  consumer: TelemetryConsumerPort;
  broadcaster: IncidentBroadcasterPort;
  detector: SlidingWindowAnomalyDetector;
  telemetryRepository?: TelemetryRepositoryPort;
  incidentRepository?: IncidentRepositoryPort;
  batchSize?: number;
  blockMs?: number;
  heartbeatIntervalMs?: number;
  logger?: {
    info(obj: unknown, msg?: string): void;
    warn(obj: unknown, msg?: string): void;
    error(obj: unknown, msg?: string): void;
  };
}

export class TelemetryEventProcessor {
  private readonly consumer: TelemetryConsumerPort;
  private readonly broadcaster: IncidentBroadcasterPort;
  private readonly detector: SlidingWindowAnomalyDetector;
  private readonly telemetryRepository?: TelemetryRepositoryPort;
  private readonly incidentRepository?: IncidentRepositoryPort;
  private readonly batchSize: number;
  private readonly blockMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly logger?: TelemetryEventProcessorConfig["logger"];

  private isRunning: boolean = false;
  private pollPromise: Promise<void> | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  constructor(config: TelemetryEventProcessorConfig) {
    this.consumer = config.consumer;
    this.broadcaster = config.broadcaster;
    this.detector = config.detector;
    this.telemetryRepository = config.telemetryRepository;
    this.incidentRepository = config.incidentRepository;
    this.batchSize = config.batchSize ?? 50;
    this.blockMs = config.blockMs ?? 2000;
    this.heartbeatIntervalMs = config.heartbeatIntervalMs ?? 10_000;
    this.logger = config.logger;
  }

  /**
   * Processes a single batch of stream messages:
   * 1. Evaluates sliding window anomaly detection.
   * 2. Broadcasts and persists any detected incidents.
   * 3. Persists telemetry events in bulk to database.
   * 4. Acknowledges message IDs in stream.
   */
  async processNextBatch(): Promise<number> {
    const messages = await this.consumer.readBatch(this.batchSize, this.blockMs);
    if (!messages || messages.length === 0) {
      return 0;
    }

    const acknowledgedIds: string[] = [];
    const validEventsToPersist: TelemetryEvent[] = [];

    for (const item of messages) {
      try {
        const incident = this.detector.processEvent(item.event);

        if (incident) {
          this.logger?.warn?.(
            { incidentId: incident.incident_id, type: incident.incident_type, device: incident.device_id },
            "Anomaly detected: Consecutive speeding incident confirmed"
          );

          if (this.incidentRepository) {
            await this.incidentRepository.save(incident);
          }
          await this.broadcaster.broadcast(incident);
        }

        validEventsToPersist.push(item.event);
        acknowledgedIds.push(item.id);
      } catch (err) {
        this.logger?.error?.(
          { messageId: item.id, err },
          "Failed to process telemetry stream message. Skipping to preserve stream progress."
        );
        acknowledgedIds.push(item.id);
      }
    }

    // High-performance batch persistence to PostgreSQL
    if (this.telemetryRepository && validEventsToPersist.length > 0) {
      try {
        await this.telemetryRepository.saveBatch(validEventsToPersist);
      } catch (err) {
        this.logger?.error?.({ err }, "Failed to persist telemetry batch to repository");
        // We do not rethrow here to prevent consumer deadlock; stream offset still advances
      }
    }

    // Acknowledge stream messages
    if (acknowledgedIds.length > 0) {
      await this.consumer.acknowledge(acknowledgedIds);
    }

    return messages.length;
  }

  /**
   * Sweeps devices for signal loss (>60s), persists and broadcasts any detected incidents.
   */
  async checkSignalLoss(currentTimeMs: number = Date.now()): Promise<IncidentDetected[]> {
    const incidents = this.detector.checkSignalLoss(currentTimeMs);

    for (const incident of incidents) {
      this.logger?.warn?.(
        { incidentId: incident.incident_id, device: incident.device_id },
        "Anomaly detected: Device signal loss confirmed (>60s)"
      );

      if (this.incidentRepository) {
        await this.incidentRepository.save(incident);
      }
      await this.broadcaster.broadcast(incident);
    }

    return incidents;
  }

  /**
   * Starts the continuous stream consumption and heartbeat sweep loop.
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    this.logger?.info?.({}, "TelemetryEventProcessor consumer loop started");

    this.pollPromise = (async () => {
      while (this.isRunning) {
        try {
          await this.processNextBatch();
        } catch (err) {
          this.logger?.error?.({ err }, "Error in stream consumption loop tick. Backing off 1000ms...");
          if (this.isRunning) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        }
      }
    })();

    this.heartbeatTimer = setInterval(async () => {
      if (!this.isRunning) return;
      try {
        await this.checkSignalLoss();
      } catch (err) {
        this.logger?.error?.({ err }, "Error checking signal loss heartbeat");
      }
    }, this.heartbeatIntervalMs);
  }

  /**
   * Gracefully stops the processor.
   */
  async stop(): Promise<void> {
    if (!this.isRunning) return;
    this.isRunning = false;

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    if (this.pollPromise) {
      await this.pollPromise;
      this.pollPromise = null;
    }

    this.logger?.info?.({}, "TelemetryEventProcessor stopped cleanly");
  }

  isProcessing(): boolean {
    return this.isRunning;
  }
}
