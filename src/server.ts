import { buildApp } from "./app.js";
import { createRedisClient } from "./infrastructure/redis/redis.connection.js";
import { createDatabasePool } from "./infrastructure/database/postgres.connection.js";
import { PgTelemetryRepository } from "./infrastructure/database/pg-telemetry.repository.js";
import { PgIncidentRepository } from "./infrastructure/database/pg-incident.repository.js";
import { RedisStreamTelemetryPublisher } from "./infrastructure/redis/redis-stream-telemetry.publisher.js";
import { RedisStreamTelemetryConsumer } from "./infrastructure/redis/redis-stream-telemetry.consumer.js";
import { WebSocketIncidentBroadcaster } from "./infrastructure/websocket/websocket-incident.broadcaster.js";
import { SlidingWindowAnomalyDetector } from "./application/services/sliding-window-anomaly.detector.js";
import { TelemetryEventProcessor } from "./application/services/telemetry-event.processor.js";

async function bootstrap(): Promise<void> {
  const PORT = Number(process.env.PORT) || 3333;
  const HOST = process.env.HOST || "0.0.0.0";
  const STREAM_NAME = process.env.TELEMETRY_STREAM_NAME || "traffic:telemetry:stream";
  const CONSUMER_GROUP = process.env.CONSUMER_GROUP_NAME || "traffic-analytics-group";

  // 1. Separate Redis Connections (Isolation between Publisher and Blocking Consumer)
  const publisherRedis = createRedisClient();
  const consumerRedis = createRedisClient();

  // 2. Initialize PostgreSQL Connection Pool
  const dbPool = createDatabasePool();
  const telemetryRepo = new PgTelemetryRepository(dbPool);
  const incidentRepo = new PgIncidentRepository(dbPool);

  // 3. Instantiate Publisher & Consumer adapters
  const publisher = new RedisStreamTelemetryPublisher(publisherRedis, {
    streamName: STREAM_NAME,
    maxStreamLength: Number(process.env.MAX_STREAM_LEN) || 1_000_000
  });

  const consumer = new RedisStreamTelemetryConsumer(consumerRedis, {
    streamName: STREAM_NAME,
    groupName: CONSUMER_GROUP
  });

  // 4. Instantiate Incident Broadcaster (WebSocket)
  const broadcaster = new WebSocketIncidentBroadcaster();

  // 5. Build Fastify application
  const app = buildApp({ publisher, broadcaster });

  // 6. Instantiate Domain Anomaly Detector & Consumer Processor
  const detector = new SlidingWindowAnomalyDetector({
    speedThresholdKmh: Number(process.env.SPEED_THRESHOLD_KMH) || 100,
    consecutiveViolationsThreshold: Number(process.env.CONSECUTIVE_SPEEDING_THRESHOLD) || 3,
    signalLossTimeoutMs: Number(process.env.SIGNAL_LOSS_TIMEOUT_MS) || 60_000 // 60s
  });

  const processor = new TelemetryEventProcessor({
    consumer,
    broadcaster,
    detector,
    telemetryRepository: telemetryRepo,
    incidentRepository: incidentRepo,
    batchSize: 50,
    blockMs: 2000,
    heartbeatIntervalMs: 10_000,
    logger: app.log
  });

  // 7. Graceful Shutdown Handler
  const shutdown = async (signal: string) => {
    app.log.warn({ signal }, "Initiating graceful shutdown sequence...");

    try {
      await processor.stop();
      app.log.info("Consumer processor stopped cleanly.");

      await app.close();
      app.log.info("HTTP & WebSocket server closed.");

      await Promise.all([publisherRedis.quit(), consumerRedis.quit()]);
      app.log.info("Redis connections closed.");

      await dbPool.end();
      app.log.info("PostgreSQL pool drained. Service exited gracefully.");
      process.exit(0);
    } catch (err) {
      app.log.error({ err }, "Error during graceful shutdown");
      process.exit(1);
    }
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  // 8. Connect, setup consumer group and listen
  try {
    await Promise.all([publisherRedis.connect(), consumerRedis.connect()]);
    app.log.info("Connected to Redis for publisher and consumer streams.");

    // Ping PostgreSQL
    const client = await dbPool.connect();
    client.release();
    app.log.info("Connected to partitioned PostgreSQL database successfully.");

    // Ensure consumer group exists
    await consumer.ensureConsumerGroup();
    app.log.info({ stream: STREAM_NAME, group: CONSUMER_GROUP }, "Consumer group verified.");

    // Start background stream worker
    processor.start();

    await app.listen({ port: PORT, host: HOST });
    app.log.info(`Traffic Telemetry & Incident Service running on http://${HOST}:${PORT}`);
    app.log.info(`WebSocket Operations stream active at ws://${HOST}:${PORT}/ws/operations`);
  } catch (err) {
    app.log.fatal({ err }, "Fatal failure bootstrapping service");
    await Promise.allSettled([
      publisherRedis.disconnect(),
      consumerRedis.disconnect(),
      dbPool.end()
    ]);
    process.exit(1);
  }
}

bootstrap().catch((err) => {
  console.error("Unhandled bootstrap error:", err);
  process.exit(1);
});
