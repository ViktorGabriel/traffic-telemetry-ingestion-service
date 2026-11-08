# Traffic Telemetry & Incident Ingestion Microservice

> Microsserviço de alta vazão para ingestão contínua de telemetria de tráfego, detecção de incidentes viários por janela deslizante (*sliding window*), broadcast via WebSocket e persistência colunar particionada em PostgreSQL. Construído com **Fastify**, **Redis Streams**, **PostgreSQL**, **TypeScript**, **Zod** e **Clean Architecture**.

---

## 🏛️ Arquitetura do Sistema

O sistema é dividido em duas esteiras desacopladas para garantir que a ingestão HTTP opere com latência mínima sem gargalos no event loop:

```
[Sensores IoT / Radares]
           │ (HTTP POST /telemetry/events em Batch)
           ▼
[Fastify Ingestion API] ──(Zod Fail-Fast)──► [Redis Stream: traffic:telemetry:stream]
                                                        │
                             ┌──────────────────────────┴──────────────────────────┐
                             ▼ (XREADGROUP / Pipelined ACK)                        │
               [TelemetryEventProcessor]                                           │
                             │                                                     │
                             ▼                                                     ▼
              [SlidingWindowAnomalyDetector]                           [PostgreSQL Particionado]
              ├── Excesso de Velocidade Consecutivo (>= 3x)            (Range por recorded_at + BRIN)
              └── Perda de Sinal de Dispositivo (> 60s)
                             │
                  (IncidentDetected Event)
                             ▼
              [WebSocketIncidentBroadcaster]
                             │
                             ▼
              [Painel de Operações ws://.../ws/operations]
```

### Principais Destaques de Engenharia:
1. **Ingestão em Lote Não-Bloqueante:** Utiliza *Redis Pipelining* (`redis.pipeline()`) agrupando centenas de `XADD` em um único frame TCP socket, garantindo que o Node.js responda em $<5\text{ms}$ sem travar o event loop.
2. **Detector de Anomalias por Janela Deslizante (*Sliding Window*):**
   - **Excesso de Velocidade Consecutivo:** Monitora o histórico recente por `device_id`. Ao detectar leituras consecutivas acima do limite, dispara alerta de incidente classificado por risco (`MEDIUM`, `HIGH`, `CRITICAL`).
   - **Detecção de Perda de Sinal (> 60s):** Varre dispositivos conhecidos e detecta ausência de sinal com mecanismo anti-spam de alertas.
3. **Persistência Particionada em Séries Temporais:**
   - PostgreSQL particionado por intervalo de tempo (`PARTITION BY RANGE (recorded_at)`).
   - Índice compactado **BRIN** (*Block Range Index*) para escaneamento ultra-rápido de faixas temporais consumindo $<1\%$ do espaço de índices convencionais B-Tree.
4. **Broadcast em Tempo Real:** Conexões ativas via WebSocket (`/ws/operations`) recebem notificações instantâneas de incidentes confirmados.

---

## 🛠️ Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| **Runtime & Linguagem** | Node.js 22 LTS / TypeScript 5.8 |
| **Framework HTTP** | Fastify 5.x |
| **Buffer / Mensageria** | Redis 7 (Streams com Consumer Groups & Pipelining) |
| **Banco de Dados** | PostgreSQL 16 (Particionamento Temporal Declarativo + BRIN) |
| **WebSockets** | `@fastify/websocket` |
| **Validação de Contratos** | Zod 3.x |
| **Suíte de Testes** | Vitest 3.x (TDD com mocks em memória) |
| **Contêineres** | Docker (Multi-stage build com `dumb-init`) & Docker Compose |

---

## 📁 Estrutura do Projeto (Clean Architecture)

```text
src/
├── domain/
│   ├── telemetry/            # Schemas Zod, tipos de eventos e portas de publicação/consumo
│   └── incidents/            # Schemas de anomalias/incidentes e portas de persistência/broadcast
├── application/
│   ├── use-cases/            # Caso de uso de ingestão em batch
│   └── services/             # Detector de janela deslizante e consumidor de telemetria
├── infrastructure/
│   ├── redis/                # Adaptadores de publicação e consumo em Redis Streams
│   ├── database/             # Pool PostgreSQL, migrações de partição e repositórios
│   ├── websocket/            # Broadcaster de incidentes em tempo real
│   └── repositories/         # Mocks em memória para execução de testes unitários isolados
├── interfaces/
│   └── http/                 # Controllers, rotas Fastify e tratamento global de erros
├── app.ts                    # Factory e injeção de dependências do Fastify
└── server.ts                 # Bootstrap e orquestração de encerramento gracioso (Graceful Shutdown)
```

---

## 🚀 Como Executar

### Pré-requisitos
- Node.js 20+ ou Docker & Docker Compose

### 1. Inicializar Infraestrutura Completa (Redis + PostgreSQL)
```bash
docker compose up -d redis postgres
```

### 2. Instalação e Execução Local
```bash
# Instalar dependências
npm install

# Modo desenvolvimento com hot-reload
npm run dev

# Compilar para produção
npm run build

# Iniciar runtime compilado
npm start
```

### 3. Execução Total via Docker Compose (App + Infra)
```bash
docker compose up -d --build
```

---

## 🧪 Testes Automatizados

O projeto adota TDD rigoroso. Nenhuma dependência externa ativa (Redis ou PostgreSQL) é necessária para rodar os testes:

```bash
# Executar todos os testes
npm test

# Modo observador (watch)
npm run test:watch
```

A suíte inclui:
- **Testes Unitários:** Validação defensiva com Zod, detecção de velocidade consecutiva, limites de batch e controle de timeouts.
- **Teste de Pipeline com 50 Eventos Sequenciais:** Simula uma frota de veículos ao longo de 75 segundos, validando a persistência em banco, reconhecimento (`XACK`) no stream, detecção de infrator e identificação de veículo sem sinal.
- **Testes de Integração HTTP:** Validação de endpoints `POST /telemetry/events` (202 Accepted, 400 Bad Request, 503 Service Unavailable).

---

## 📄 Licença

Distribuído sob a licença MIT. Desenvolvido por [Viktor Gabriel](https://github.com/ViktorGabriel).
