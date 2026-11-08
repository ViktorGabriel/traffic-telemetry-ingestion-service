import { Redis } from "ioredis";
import { TelemetryConsumerPort, StreamMessage } from "../../domain/telemetry/telemetry-consumer.port.js";
import { TelemetryEvent } from "../../domain/telemetry/telemetry-event.schema.js";

export interface RedisStreamConsumerOptions {
  streamName?: string;
  groupName?: string;
  consumerName?: string;
}

export class RedisStreamTelemetryConsumer implements TelemetryConsumerPort {
  private readonly streamName: string;
  private readonly groupName: string;
  private readonly consumerName: string;

  constructor(
    private readonly redis: Redis,
    options: RedisStreamConsumerOptions = {}
  ) {
    this.streamName = options.streamName || process.env.TELEMETRY_STREAM_NAME || "traffic:telemetry:stream";
    this.groupName = options.groupName || process.env.CONSUMER_GROUP_NAME || "traffic-analytics-group";
    this.consumerName =
      options.consumerName ||
      process.env.CONSUMER_NAME ||
      `worker-${process.pid}-${Math.random().toString(36).substring(2, 7)}`;
  }

  /**
   * Ensures consumer group exists using MKSTREAM.
   */
  async ensureConsumerGroup(): Promise<void> {
    try {
      await this.redis.xgroup("CREATE", this.streamName, this.groupName, "$", "MKSTREAM");
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      if (!message.includes("BUSYGROUP")) {
        throw err;
      }
      // BUSYGROUP means consumer group already exists, which is normal and expected.
    }
  }

  /**
   * Reads pending/new messages from the Redis Stream for this consumer group.
   */
  async readBatch(count: number = 50, blockMs: number = 2000): Promise<StreamMessage[]> {
    // XREADGROUP GROUP <group> <consumer> COUNT <count> BLOCK <blockMs> STREAMS <stream> >
    const rawResults = (await this.redis.xreadgroup(
      "GROUP",
      this.groupName,
      this.consumerName,
      "COUNT",
      count,
      "BLOCK",
      blockMs,
      "STREAMS",
      this.streamName,
      ">"
    )) as Array<[string, Array<[string, string[]]>]> | null;

    if (!rawResults || !Array.isArray(rawResults) || rawResults.length === 0) {
      return [];
    }

    const messages: StreamMessage[] = [];
    const [_streamKey, streamEntries] = rawResults[0];

    for (const [entryId, fields] of streamEntries) {
      try {
        const eventObj: Record<string, unknown> = {};

        // Parse key-value pairs from Redis stream entry
        for (let i = 0; i < fields.length; i += 2) {
          const key = fields[i];
          const val = fields[i + 1];

          if (key === "speed_kmh") {
            eventObj.speed_kmh = Number(val);
          } else if (key === "payload") {
            try {
              eventObj.payload = JSON.parse(val);
            } catch {
              eventObj.payload = {};
            }
          } else {
            eventObj[key] = val;
          }
        }

        messages.push({
          id: entryId,
          event: eventObj as unknown as TelemetryEvent
        });
      } catch {
        // Skip unparseable stream entry
        continue;
      }
    }

    return messages;
  }

  /**
   * Acknowledges message IDs in Redis Stream.
   */
  async acknowledge(messageIds: string[]): Promise<number> {
    if (messageIds.length === 0) return 0;
    return await this.redis.xack(this.streamName, this.groupName, ...messageIds);
  }
}
