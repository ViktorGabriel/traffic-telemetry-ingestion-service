import type { WebSocket } from "ws";
import { IncidentBroadcasterPort } from "../../domain/incidents/incident-broadcaster.port.js";
import { IncidentDetected } from "../../domain/incidents/incident.schema.js";

export class WebSocketIncidentBroadcaster implements IncidentBroadcasterPort {
  private readonly clients = new Set<WebSocket>();

  /**
   * Registers a newly connected operator WebSocket.
   */
  registerClient(socket: WebSocket): void {
    this.clients.add(socket);

    socket.on("close", () => {
      this.clients.delete(socket);
    });

    socket.on("error", () => {
      this.clients.delete(socket);
    });
  }

  /**
   * Broadcasts an incident alert to all connected active WebSocket clients.
   */
  async broadcast(incident: IncidentDetected): Promise<void> {
    if (this.clients.size === 0) return;

    const payload = JSON.stringify({
      channel: "INCIDENTS_ALERT",
      data: incident
    });

    const deadSockets: WebSocket[] = [];

    for (const socket of this.clients) {
      // 1 = WebSocket.OPEN
      if (socket.readyState === 1) {
        try {
          socket.send(payload);
        } catch {
          deadSockets.push(socket);
        }
      } else {
        deadSockets.push(socket);
      }
    }

    // Clean up closed/dead sockets
    for (const dead of deadSockets) {
      this.clients.delete(dead);
    }
  }

  getActiveConnectionsCount(): number {
    return this.clients.size;
  }
}
