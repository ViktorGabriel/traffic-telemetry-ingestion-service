import { Pool } from "pg";
import { TelemetryRepositoryPort } from "../../domain/telemetry/telemetry-repository.port.js";
import { TelemetryBatch } from "../../domain/telemetry/telemetry-event.schema.js";

export class PgTelemetryRepository implements TelemetryRepositoryPort {
  constructor(private readonly pool: Pool) {}

  /**
   * Persists a batch of telemetry events into the partitioned PostgreSQL table.
   * Uses a single multi-row parameterized query for maximum throughput and minimal lock time.
   */
  async saveBatch(events: TelemetryBatch): Promise<number> {
    if (events.length === 0) return 0;

    const values: unknown[] = [];
    const rowPlaceholders: string[] = [];

    let paramIndex = 1;

    for (const event of events) {
      const coords = event.payload.coordinates;
      const latitude = coords?.latitude ?? null;
      const longitude = coords?.longitude ?? null;
      const headingDeg = coords?.heading_deg ?? null;

      rowPlaceholders.push(
        `($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`
      );

      values.push(
        event.device_id,
        event.event_type,
        event.speed_kmh,
        latitude,
        longitude,
        headingDeg,
        JSON.stringify(event.payload),
        new Date(event.timestamp)
      );
    }

    const query = `
      INSERT INTO telemetry_events (
        device_id,
        event_type,
        speed_kmh,
        latitude,
        longitude,
        heading_deg,
        payload,
        recorded_at
      ) VALUES ${rowPlaceholders.join(", ")}
    `;

    const result = await this.pool.query(query, values);
    return result.rowCount ?? events.length;
  }
}
