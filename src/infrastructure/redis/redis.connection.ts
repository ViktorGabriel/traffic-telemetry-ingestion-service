import { Redis, RedisOptions } from "ioredis";

export interface RedisConfig {
  host?: string;
  port?: number;
  password?: string;
  commandTimeoutMs?: number;
  connectTimeoutMs?: number;
  maxRetriesPerRequest?: number;
}

export function createRedisClient(config: RedisConfig = {}): Redis {
  const options: RedisOptions = {
    host: config.host || process.env.REDIS_HOST || "127.0.0.1",
    port: config.port || Number(process.env.REDIS_PORT) || 6379,
    password: config.password || process.env.REDIS_PASSWORD || undefined,
    commandTimeout: config.commandTimeoutMs || 3000,
    connectTimeout: config.connectTimeoutMs || 5000,
    maxRetriesPerRequest: config.maxRetriesPerRequest ?? 2,
    enableReadyCheck: true,
    reconnectOnError(err) {
      const targetErrors = ["READONLY", "ETIMEDOUT", "ECONNRESET"];
      return targetErrors.some((target) => err.message.includes(target));
    },
    retryStrategy(times) {
      const delay = Math.min(times * 100, 3000);
      return delay;
    },
    lazyConnect: true
  };

  return new Redis(options);
}
