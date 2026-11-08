# Traffic Telemetry & Incident Ingestion Service

> Microsserviço de alta vazão para ingestão contínua de telemetria de tráfego, radares e detecção de incidentes viários em tempo real, construído com **Fastify**, **Redis Streams (ioredis)**, validação defensiva em **Zod** e arquitetura hexagonal / Clean Architecture.

## 🏛️ Arquitetura & Visão Geral

O **Traffic Telemetry Ingestion Service** foi projetado para suportar cargas elevadas de leitura e telemetria geradas por sensores de fluxo viário e dispositivos IoT. Os dados recebidos passam por validação estrita de contratos antes de serem despachados de forma não bloqueante para um stream do Redis (`XADD`), permitindo que workers distribuídos processem métricas, anomalias e incidentes de forma assíncrona.

### Principais Capacidades:
- **Ingestão em Lote e Individual:** Suporte à ingestão de telemetrias pontuais ou em lote (`batch`) para redução de overhead HTTP.
- **Buffer de Alta Performance:** Publicação de eventos em **Redis Streams** garantindo ordenação cronológica e desacoplamento do consumidor.
- **Portas & Adaptadores (Hexagonal):** Porta de publicação (`TelemetryPublisherPort`) com implementações desacopladas para Redis Stream e mock em memória (`InMemoryTelemetryPublisher`) para testes rápidos.
- **Validação Defensiva com Zod:** Rejeição de payloads corrompidos ou com campos ausentes em nível de schema antes de qualquer acesso I/O.

---

## 🛠️ Stack Tecnológica

- **Runtime:** Node.js (>=20.0.0) com TypeScript 5.8
- **Framework HTTP:** Fastify 5.x
- **Mensageria / Streaming:** Redis Streams via `ioredis` 5.x
- **Validação de Dados:** Zod 3.x
- **Testes & Asserções:** Vitest 3.x
- **Infraestrutura Local:** Docker & Docker Compose

---

## 📁 Estrutura do Projeto

```text
src/
├── application/
│   └── use-cases/            # Caso de uso: ingestão em lote / unitária
├── domain/
│   └── telemetry/            # Schemas de evento de telemetria e portas (interfaces)
├── infrastructure/
│   ├── redis/                # Conexão e publicação via Redis Streams
│   └── repositories/         # Implementação em memória para testes
├── interfaces/
│   └── http/                 # Controllers, Middlewares de erro e Rotas Fastify
├── app.ts                    # Factory da aplicação Fastify
└── server.ts                 # Bootstrap e inicialização do servidor
```

---

## 🚀 Como Executar

### Pré-requisitos
- Node.js 20+
- Docker e Docker Compose

### Instalação
```bash
npm install
```

### Inicialização da Infraestrutura (Redis)
```bash
docker compose up -d
```

### Execução da Aplicação
```bash
# Modo de desenvolvimento com recarregamento a quente
npm run dev

# Compilar para produção
npm run build

# Iniciar build compilado
npm start
```

### Testes Automatizados
```bash
# Executar todos os testes com Vitest
npm test

# Modo interativo de testes
npm run test:watch
```

---

## 📄 Licença

Distribuído sob a licença MIT. Desenvolvido por [Viktor Gabriel](https://github.com/ViktorGabriel).
