import { TelemetryEvent } from "./telemetry-event.schema.js";

export interface StreamMessage {
  id: string;
  event: TelemetryEvent;
}

export interface TelemetryConsumerPort {
  /**
   * Reads a batch of pending/new messages from the stream for the specified consumer.
   */
  readBatch(count?: number, blockMs?: number): Promise<StreamMessage[]>;

  /**
   * Acknowledges successfully processed message IDs.
   */
  acknowledge(messageIds: string[]): Promise<number>;
}
