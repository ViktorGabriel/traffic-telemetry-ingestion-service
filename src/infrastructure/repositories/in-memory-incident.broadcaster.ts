import { IncidentBroadcasterPort } from "../../domain/incidents/incident-broadcaster.port.js";
import { IncidentDetected } from "../../domain/incidents/incident.schema.js";

export class InMemoryIncidentBroadcaster implements IncidentBroadcasterPort {
  public broadcastedIncidents: IncidentDetected[] = [];
  public connectionsCount: number = 1;

  async broadcast(incident: IncidentDetected): Promise<void> {
    this.broadcastedIncidents.push(incident);
  }

  getActiveConnectionsCount(): number {
    return this.connectionsCount;
  }

  clear(): void {
    this.broadcastedIncidents = [];
  }
}
