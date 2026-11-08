import { FastifyInstance } from "fastify";
import { TelemetryController } from "../controllers/telemetry.controller.js";
import { WebSocketIncidentBroadcaster } from "../../../infrastructure/websocket/websocket-incident.broadcaster.js";

export interface TelemetryRoutesOptions {
  controller: TelemetryController;
  broadcaster?: WebSocketIncidentBroadcaster;
}

export async function telemetryRoutes(
  fastify: FastifyInstance,
  options: TelemetryRoutesOptions
): Promise<void> {
  const { controller, broadcaster } = options;

  // Ingestion Endpoint
  fastify.post(
    "/telemetry/events",
    {
      config: {
        rateLimit: {
          max: 1000,
          timeWindow: "1 minute"
        }
      }
    },
    controller.ingestBatch.bind(controller)
  );

  // Operations Dashboard WebSocket Broadcast Endpoint
  if (broadcaster) {
    fastify.get(
      "/ws/operations",
      { websocket: true },
      (socket /* WebSocket */, _request) => {
        broadcaster.registerClient(socket);
        socket.send(
          JSON.stringify({
            event: "CONNECTED",
            message: "Connected to Operations Incident Stream",
            timestamp: new Date().toISOString()
          })
        );
      }
    );
  }

  // Healthcheck Endpoint
  fastify.get("/health", async (_request, reply) => {
    reply.send({
      status: "UP",
      timestamp: new Date().toISOString(),
      active_ws_subscribers: broadcaster?.getActiveConnectionsCount() ?? 0
    });
  });
}
