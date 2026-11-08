import fastify, { FastifyInstance } from "fastify";
import { TelemetryPublisherPort } from "./domain/telemetry/telemetry-publisher.port.js";
import { IngestTelemetryBatchUseCase } from "./application/use-cases/ingest-telemetry-batch.use-case.js";
import { TelemetryController } from "./interfaces/http/controllers/telemetry.controller.js";
import { telemetryRoutes } from "./interfaces/http/routes/telemetry.routes.js";
import { globalErrorHandler } from "./interfaces/http/middlewares/error-handler.js";

export interface AppOptions {
  publisher: TelemetryPublisherPort;
  bodyLimitBytes?: number;
  logger?: boolean | object;
}

export function buildApp(options: AppOptions): FastifyInstance {
  const app = fastify({
    logger: options.logger ?? {
      level: process.env.LOG_LEVEL || "info",
      transport:
        process.env.NODE_ENV === "development"
          ? {
              target: "pino-pretty",
              options: { colorize: true }
            }
          : undefined
    },
    // Prevent unconstrained memory consumption by batch payloads (max 5MB default)
    bodyLimit: options.bodyLimitBytes || 5 * 1024 * 1024,
    requestIdHeader: "x-correlation-id"
  });

  // Global Error Handler
  app.setErrorHandler(globalErrorHandler);

  // Dependency Injection
  const ingestUseCase = new IngestTelemetryBatchUseCase(options.publisher);
  const controller = new TelemetryController(ingestUseCase);

  // Register Routes
  app.register(telemetryRoutes, { controller });

  return app;
}
