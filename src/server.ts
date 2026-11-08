import { buildApp } from "./app.js";
import { createRedisClient } from "./infrastructure/redis/redis.connection.js";
import { RedisStreamTelemetryPublisher } from "./infrastructure/redis/redis-stream-telemetry.publisher.js";

async function bootstrap(): Promise<void> {
  const PORT = Number(process.env.PORT) || 3333;
  const HOST = process.env.HOST || "0.0.0.0";

  // 1. Initialize Redis connection with resilience options
  const redis = createRedisClient();

  // 2. Instantiate Stream Publisher
  const publisher = new RedisStreamTelemetryPublisher(redis, {
    streamName: process.env.TELEMETRY_STREAM_NAME || "traffic:telemetry:stream",
    maxStreamLength: Number(process.env.MAX_STREAM_LEN) || 1_000_000
  });

  // 3. Build Fastify application
  const app = buildApp({ publisher });

  // 4. Graceful Shutdown Handler
  const shutdown = async (signal: string) => {
    app.log.warn({ signal }, "Initiating graceful shutdown sequence...");

    try {
      await app.close();
      app.log.info("HTTP server closed. Flushing and disconnecting Redis...");
      await redis.quit();
      app.log.info("Redis connection closed cleanly. Service stopped.");
      process.exit(0);
    } catch (err) {
      app.log.error({ err }, "Error during graceful shutdown");
      process.exit(1);
    }
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  // 5. Connect and listen
  try {
    await redis.connect();
    app.log.info("Connected to Redis Streams cluster/node successfully.");

    await app.listen({ port: PORT, host: HOST });
    app.log.info(`Traffic Telemetry Ingestion Service listening on http://${HOST}:${PORT}`);
  } catch (err) {
    app.log.fatal({ err }, "Fatal failure bootstrapping service");
    await redis.disconnect();
    process.exit(1);
  }
}

bootstrap().catch((err) => {
  console.error("Unhandled bootstrap error:", err);
  process.exit(1);
});
