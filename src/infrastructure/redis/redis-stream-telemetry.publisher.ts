import { Redis } from "ioredis";
import { TelemetryPublisherPort, PublishResult } from "../../domain/telemetry/telemetry-publisher.port.js";
import { TelemetryBatch, TelemetryEvent } from "../../domain/telemetry/telemetry-event.schema.js";

export interface RedisStreamPublisherOptions {
  streamName?: string;
  maxStreamLength?: number;
}

export class RedisStreamTelemetryPublisher implements TelemetryPublisherPort {
  private readonly streamName: string;
  private readonly maxStreamLength: number;

  constructor(
    private readonly redis: Redis,
    options: RedisStreamPublisherOptions = {}
  ) {
    this.streamName = options.streamName || process.env.TELEMETRY_STREAM_NAME || "traffic:telemetry:stream";
    this.maxStreamLength = options.maxStreamLength || 1_000_000;
  }

  /**
   * Publishes a batch to Redis Streams using Redis Pipelining.
   * This sends multiple XADD commands in a single TCP socket packet,
   * avoiding event loop blocking and minimizing network roundtrips.
   */
  async publishBatch(events: TelemetryBatch): Promise<PublishResult> {
    const startTime = performance.now();
    const pipeline = this.redis.pipeline();

    for (const event of events) {
      pipeline.xadd(
        this.streamName,
        "MAXLEN",
        "~",
        this.maxStreamLength,
        "*",
        "device_id",
        event.device_id,
        "event_type",
        event.event_type,
        "speed_kmh",
        event.speed_kmh.toString(),
        "timestamp",
        event.timestamp,
        "payload",
        JSON.stringify(event.payload)
      );
    }

    const responses = await pipeline.exec();

    if (!responses) {
      throw new Error(`Pipeline execution failed: empty response from Redis stream [${this.streamName}]`);
    }

    // Inspect individual command execution results from pipeline
    for (let i = 0; i < responses.length; i++) {
      const [err] = responses[i];
      if (err) {
        throw new Error(`Failed to publish event index ${i} to stream: ${err.message}`);
      }
    }

    const duration_ms = Number((performance.now() - startTime).toFixed(2));

    return {
      published_count: events.length,
      stream_name: this.streamName,
      duration_ms
    };
  }

  /**
   * Publishes a single event directly to the stream.
   */
  async publishSingle(event: TelemetryEvent): Promise<string> {
    const entryId = await this.redis.xadd(
      this.streamName,
      "MAXLEN",
      "~",
      this.maxStreamLength,
      "*",
      "device_id",
      event.device_id,
      "event_type",
      event.event_type,
      "speed_kmh",
      event.speed_kmh.toString(),
      "timestamp",
      event.timestamp,
      "payload",
      JSON.stringify(event.payload)
    );

    if (!entryId) {
      throw new Error(`Failed to publish single event to stream [${this.streamName}]`);
    }

    return entryId;
  }
}
