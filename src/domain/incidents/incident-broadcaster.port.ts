import { IncidentDetected } from "./incident.schema.js";

export interface IncidentBroadcasterPort {
  /**
   * Broadcasts an incident alert to all connected operators in real time.
   */
  broadcast(incident: IncidentDetected): Promise<void>;
  
  /**
   * Returns active client connections count.
   */
  getActiveConnectionsCount(): number;
}
