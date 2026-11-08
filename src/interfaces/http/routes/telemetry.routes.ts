import { FastifyInstance } from "fastify";
import { TelemetryController } from "../controllers/telemetry.controller.js";

export async function telemetryRoutes(
  fastify: FastifyInstance,
  options: { controller: TelemetryController }
): Promise<void> {
  const { controller } = options;

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

  fastify.get("/health", async (_request, reply) => {
    reply.send({ status: "UP", timestamp: new Date().toISOString() });
  });
}
