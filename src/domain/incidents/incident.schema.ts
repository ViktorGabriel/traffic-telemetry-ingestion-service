import { z } from "zod";
import { GeoCoordinatesSchema } from "../telemetry/telemetry-event.schema.js";

export const IncidentTypeEnum = z.enum([
  "CONSECUTIVE_SPEEDING",
  "SIGNAL_LOSS"
]);

export type IncidentType = z.infer<typeof IncidentTypeEnum>;

export const IncidentSeverityEnum = z.enum([
  "LOW",
  "MEDIUM",
  "HIGH",
  "CRITICAL"
]);

export type IncidentSeverity = z.infer<typeof IncidentSeverityEnum>;

export const IncidentDetectedSchema = z.object({
  incident_id: z.string().min(1),
  device_id: z.string().min(1),
  incident_type: IncidentTypeEnum,
  severity: IncidentSeverityEnum,
  detected_at: z.string().datetime({ offset: true }),
  details: z.object({
    description: z.string(),
    speed_kmh: z.number().optional(),
    speed_limit_kmh: z.number().optional(),
    consecutive_violations: z.number().optional(),
    speed_readings: z.array(z.number()).optional(),
    last_seen_at: z.string().optional(),
    elapsed_seconds_without_signal: z.number().optional(),
    coordinates: GeoCoordinatesSchema.optional(),
    extra: z.record(z.string(), z.unknown()).optional()
  })
});

export type IncidentDetected = z.infer<typeof IncidentDetectedSchema>;
