import { z } from "zod";

/**
 * GeoCoordinates Schema for geographic positioning.
 */
export const GeoCoordinatesSchema = z.object({
  latitude: z.number().min(-90).max(90),
  longitude: z.number().min(-180).max(180),
  heading_deg: z.number().min(0).max(360).optional(),
  altitude_m: z.number().optional()
});

export type GeoCoordinates = z.infer<typeof GeoCoordinatesSchema>;

/**
 * Known telemetry event types in traffic monitoring.
 */
export const TelemetryEventTypeEnum = z.enum([
  "SPEED_UPDATE",
  "INCIDENT_ALERT",
  "HARD_BRAKE",
  "CONGESTION_LEVEL",
  "ROAD_HAZARD",
  "DEVICE_HEARTBEAT"
]);

export type TelemetryEventType = z.infer<typeof TelemetryEventTypeEnum>;

/**
 * Single Telemetry & Incident Event Schema.
 */
export const TelemetryEventSchema = z.object({
  device_id: z
    .string({ required_error: "device_id is required" })
    .trim()
    .min(3, "device_id must be at least 3 characters")
    .max(64, "device_id must not exceed 64 characters")
    .regex(/^[a-zA-Z0-9_\-:]+$/, "device_id must contain only alphanumeric characters, hyphens, colons, or underscores"),
  
  timestamp: z
    .string({ required_error: "timestamp is required" })
    .datetime({ offset: true, message: "timestamp must be an ISO-8601 string" }),
  
  event_type: z.union([
    TelemetryEventTypeEnum,
    z.string().min(3).max(50)
  ]),
  
  speed_kmh: z
    .number({ required_error: "speed_kmh is required" })
    .min(0, "speed_kmh cannot be negative")
    .max(350, "speed_kmh exceeds maximum realistic limit (350 km/h)"),
  
  payload: z.object({
    coordinates: GeoCoordinatesSchema.optional(),
    severity: z.enum(["LOW", "MEDIUM", "HIGH", "CRITICAL"]).optional(),
    metadata: z.record(z.string(), z.unknown()).optional()
  }).default({})
});

export type TelemetryEvent = z.infer<typeof TelemetryEventSchema>;

/**
 * Maximum batch size to prevent event loop starvation and heap allocation spikes.
 */
export const MAX_BATCH_SIZE = 2000;

/**
 * Ingestion Batch Schema.
 */
export const TelemetryBatchSchema = z
  .array(TelemetryEventSchema)
  .min(1, "Batch must contain at least 1 event")
  .max(MAX_BATCH_SIZE, `Batch size cannot exceed ${MAX_BATCH_SIZE} events per request`);

export type TelemetryBatch = z.infer<typeof TelemetryBatchSchema>;
