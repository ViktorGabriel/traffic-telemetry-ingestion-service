# syntax=docker/dockerfile:1.4
# ==========================================
# STAGE 1: Build & TypeScript Compilation
# ==========================================
FROM node:22-alpine AS builder

WORKDIR /app

# Install build dependencies
COPY package*.json tsconfig.json ./
RUN npm ci

# Copy application source code
COPY src/ ./src/

# Compile TypeScript to JavaScript in dist/
RUN npm run build

# ==========================================
# STAGE 2: Production Dependencies Only
# ==========================================
FROM node:22-alpine AS prod-deps

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

# ==========================================
# STAGE 3: Minimal Production Runtime
# ==========================================
FROM node:22-alpine AS runner

# Add dumb-init for reliable kernel signal handling (PID 1 zombie/exit handling)
RUN apk add --no-cache dumb-init

WORKDIR /app

ENV NODE_ENV=production \
    PORT=3333 \
    HOST=0.0.0.0

# Copy production artifacts and dependencies
COPY --from=prod-deps --chown=node:node /app/node_modules ./node_modules
COPY --from=builder --chown=node:node /app/dist ./dist
COPY --from=builder --chown=node:node /app/package.json ./package.json

# Run as non-privileged unroot user
USER node

EXPOSE 3333

# Native healthcheck querying the Fastify health endpoint
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "fetch('http://localhost:3333/health').then(r => r.ok ? process.exit(0) : process.exit(1)).catch(() => process.exit(1))"

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["node", "dist/server.js"]
