import { Pool, PoolConfig } from "pg";

export interface DatabaseConfig {
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  maxConnections?: number;
  idleTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
}

export function createDatabasePool(config: DatabaseConfig = {}): Pool {
  const poolConfig: PoolConfig = {
    host: config.host || process.env.PG_HOST || "127.0.0.1",
    port: config.port || Number(process.env.PG_PORT) || 5432,
    database: config.database || process.env.PG_DATABASE || "traffic_telemetry",
    user: config.user || process.env.PG_USER || "postgres",
    password: config.password || process.env.PG_PASSWORD || "postgres",
    max: config.maxConnections || Number(process.env.PG_POOL_MAX) || 20,
    idleTimeoutMillis: config.idleTimeoutMillis || 30_000,
    connectionTimeoutMillis: config.connectionTimeoutMillis || 5_000
  };

  const pool = new Pool(poolConfig);

  pool.on("error", (err) => {
    console.error("Unexpected error on idle PostgreSQL client", err);
  });

  return pool;
}
