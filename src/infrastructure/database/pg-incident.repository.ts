import { Pool } from "pg";
import { IncidentRepositoryPort } from "../../domain/incidents/incident-repository.port.js";
import { IncidentDetected } from "../../domain/incidents/incident.schema.js";

export class PgIncidentRepository implements IncidentRepositoryPort {
  constructor(private readonly pool: Pool) {}

  /**
   * Persists a confirmed incident to PostgreSQL.
   */
  async save(incident: IncidentDetected): Promise<void> {
    const query = `
      INSERT INTO incidents (
        id,
        device_id,
        incident_type,
        severity,
        details,
        detected_at
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (id) DO NOTHING
    `;

    await this.pool.query(query, [
      incident.incident_id,
      incident.device_id,
      incident.incident_type,
      incident.severity,
      JSON.stringify(incident.details),
      new Date(incident.detected_at)
    ]);
  }

  /**
   * Retrieves the most recent detected incidents.
   */
  async findRecent(limit: number = 20): Promise<IncidentDetected[]> {
    const query = `
      SELECT 
        id as incident_id,
        device_id,
        incident_type,
        severity,
        details,
        detected_at
      FROM incidents
      ORDER BY detected_at DESC
      LIMIT $1
    `;

    const result = await this.pool.query(query, [limit]);

    return result.rows.map((row) => ({
      incident_id: row.incident_id,
      device_id: row.device_id,
      incident_type: row.incident_type,
      severity: row.severity,
      detected_at: new Date(row.detected_at).toISOString(),
      details: typeof row.details === "string" ? JSON.parse(row.details) : row.details
    }));
  }
}
