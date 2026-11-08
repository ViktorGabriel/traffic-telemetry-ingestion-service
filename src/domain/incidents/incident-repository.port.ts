import { IncidentDetected } from "./incident.schema.js";

export interface IncidentRepositoryPort {
  /**
   * Persists a confirmed incident to PostgreSQL.
   */
  save(incident: IncidentDetected): Promise<void>;

  /**
   * Retrieves the most recent detected incidents.
   */
  findRecent(limit?: number): Promise<IncidentDetected[]>;
}
