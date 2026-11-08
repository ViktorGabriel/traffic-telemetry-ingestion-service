import { TelemetryConsumerPort, StreamMessage } from "../../domain/telemetry/telemetry-consumer.port.js";

export class InMemoryTelemetryConsumer implements TelemetryConsumerPort {
  public queue: StreamMessage[] = [];
  public acknowledgedIds: string[] = [];

  async readBatch(count: number = 50, _blockMs: number = 100): Promise<StreamMessage[]> {
    const messages = this.queue.splice(0, count);
    return messages;
  }

  async acknowledge(messageIds: string[]): Promise<number> {
    this.acknowledgedIds.push(...messageIds);
    return messageIds.length;
  }

  addMessage(msg: StreamMessage): void {
    this.queue.push(msg);
  }

  clear(): void {
    this.queue = [];
    this.acknowledgedIds = [];
  }
}
