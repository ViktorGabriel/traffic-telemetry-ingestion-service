import { IncidentRepositoryPort } from "../../domain/incidents/incident-repository.port.js";
import { IncidentDetected } from "../../domain/incidents/incident.schema.js";

export class InMemoryIncidentRepository implements IncidentRepositoryPort {
  public storedIncidents: IncidentDetected[] = [];

  async save(incident: IncidentDetected): Promise<void> {
    this.storedIncidents.push(incident);
  }

  async findRecent(limit: number = 20): Promise<IncidentDetected[]> {
    return this.storedIncidents.slice(-limit).reverse();
  }

  clear(): void {
    this.storedIncidents = [];
  }
}
