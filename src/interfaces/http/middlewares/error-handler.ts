import { FastifyError, FastifyReply, FastifyRequest } from "fastify";
import { ZodError } from "zod";

export function globalErrorHandler(
  error: FastifyError | Error,
  request: FastifyRequest,
  reply: FastifyReply
): void {
  const correlationId = request.id;

  // 1. Zod Validation Errors (Defensive fail-fast)
  if (error instanceof ZodError) {
    request.log.warn({
      correlationId,
      err: error.issues,
      msg: "Validation failed for incoming payload"
    });

    reply.status(400).send({
      statusCode: 400,
      error: "Bad Request",
      message: "Validation failed for telemetry batch",
      correlationId,
      details: error.issues.map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
        code: issue.code
      }))
    });
    return;
  }

  // 2. Fastify native errors (e.g. malformed JSON or payload limits)
  if ("statusCode" in error && typeof error.statusCode === "number") {
    request.log.warn({
      correlationId,
      statusCode: error.statusCode,
      errMessage: error.message
    });

    reply.status(error.statusCode).send({
      statusCode: error.statusCode,
      error: error.name || "Request Error",
      message: error.message,
      correlationId
    });
    return;
  }

  // 3. Redis / Network I/O Failures & Backpressure
  const errorMessage = error.message.toLowerCase();
  const isRedisError =
    errorMessage.includes("redis") ||
    errorMessage.includes("econnrefused") ||
    errorMessage.includes("etimedout") ||
    errorMessage.includes("command timed out");

  if (isRedisError) {
    request.log.error({
      correlationId,
      err: error.stack,
      msg: "Infrastructure I/O failure communicating with Redis stream"
    });

    reply.status(503).send({
      statusCode: 503,
      error: "Service Unavailable",
      message: "Temporary failure buffering telemetry stream. Upstream backpressure active.",
      correlationId
    });
    return;
  }

  // 4. Fallback: Internal Server Error
  request.log.error({
    correlationId,
    err: error.stack,
    msg: "Unhandled internal server error during ingestion"
  });

  reply.status(500).send({
    statusCode: 500,
    error: "Internal Server Error",
    message: "An unexpected error occurred while processing telemetry events",
    correlationId
  });
}
