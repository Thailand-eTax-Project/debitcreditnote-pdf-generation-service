# Debit/Credit Note PDF Generation Service

A Spring Boot microservice for generating PDF/A-3 documents for Thai e-Tax debit/credit notes with embedded XML attachments. This service participates in a Saga Orchestration pattern coordinated by `orchestrator-service`.

## Tech Stack

| Component | Version |
|-----------|---------|
| Java | 21 |
| Spring Boot | 3.2.5 |
| Spring Cloud | 2023.0.1 |
| Apache Camel | 4.14.4 |
| Apache FOP | 2.9 |
| Apache PDFBox | 3.0.1 |
| PostgreSQL | 16+ |
| Kafka | Latest |

## Features

- **PDF/A-3 Generation** with embedded XML using Apache FOP and PDFBox
- **Thai Font Support** via THSarabunNew and NotoSansThaiLooped
- **Saga Orchestration** for coordinated transaction processing
- **Transactional Outbox Pattern** for reliable event publishing
- **Hexagonal Architecture** (Ports & Adapters pattern)
- **Domain-Driven Design** with clean layer separation
- **Idempotent Processing** with automatic retry handling
- **Circuit Breaker** for external service calls (MinIO, signed XML fetch)
- **Fire-and-Forget Archival** via `document.archive` topic

## Architecture

This service follows **Hexagonal Architecture** (Ports & Adapters) with Domain-Driven Design:

```
Application Layer
  ├── Inbound Ports (ProcessDebitCreditNotePdfUseCase, CompensateDebitCreditNotePdfUseCase)
  ├── Outbound Ports (PdfStoragePort, SagaReplyPort, SignedXmlFetchPort, PdfEventPort, DocumentArchivePort)
  └── DebitCreditNotePdfDocumentService (orchestration)

Domain Layer
  ├── Model (DebitCreditNotePdfDocument aggregate root)
  ├── Service (DebitCreditNotePdfGenerationService)
  └── Exception (DebitCreditNotePdfGenerationException)

Infrastructure Layer
  ├── Adapter/In (SagaCommandHandler, SagaRouteConfig — Apache Camel + Kafka)
  └── Adapter/Out (FOP PDF generator, MinIO storage, JPA persistence, outbox messaging)
```

### Saga Step

**`GENERATE_DEBIT_CREDIT_NOTE_PDF`** — The orchestrator sends this command after the debit/credit note XML has been signed by `xml-signing-service`. The service downloads the signed XML, generates a PDF/A-3 with the XML embedded, uploads to MinIO, and emits `document.archive` for fire-and-forget archival.

## Prerequisites

- **Java 21+**
- **Maven 3.6+**
- **PostgreSQL 16+** with database `debitcreditnotepdf_db`
- **Kafka** on `localhost:9092`
- **MinIO** on `localhost:9000` with bucket `debitcreditnotes`
- **teda library** installed (`cd ../../teda && mvn clean install`)
- **saga-commons library** installed (`cd ../../saga-commons && mvn clean install`)

## Quick Start

```bash
# Install dependencies
cd ../../teda && mvn clean install
cd ../saga-commons && mvn clean install

# Build and run
cd debitcreditnote-pdf-generation-service
mvn clean package
mvn spring-boot:run

# Run tests
mvn clean test -q    # All tests
mvn verify            # Tests + JaCoCo 90% line coverage check
mvn test -Dtest=DebitCreditNotePdfDocumentTest
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `debitcreditnotepdf_db` | Database name |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMERS_COUNT` | `3` | Camel concurrent consumers |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET_NAME` | `debitcreditnotes` | Target bucket name |
| `PDF_MAX_CONCURRENT_RENDERS` | `3` | Max concurrent FOP render slots |
| `PDF_GENERATION_MAX_RETRIES` | `3` | Saga retry limit per debit/credit note |

### Database Setup

```bash
mvn flyway:migrate
mvn flyway:info
```

## Kafka Topics

### Inbound (Consumed)

| Topic | Purpose | Consumer |
|-------|---------|----------|
| `saga.command.debit-credit-note-pdf` | Triggers PDF generation | `SagaCommandHandler#handle(process)` |
| `saga.compensation.debit-credit-note-pdf` | Rollback: delete PDF from MinIO and DB record | `SagaCommandHandler#handle(compensate)` |

### Outbound (Produced via Outbox)

| Topic | Purpose | Producer |
|-------|---------|----------|
| `saga.reply.debit-credit-note-pdf` | SUCCESS / FAILURE / COMPENSATED reply to orchestrator | `SagaReplyPublisher` |
| `document.archive` | Fire-and-forget archival event for UNSIGNED_PDF | `OutboxDocumentArchiveAdapter` |

### Command Format

```json
{
  "sagaId": "550e8400-e29b-41d4-a716-446655440000",
  "sagaStep": "GENERATE_DEBIT_CREDIT_NOTE_PDF",
  "correlationId": "660e8400-e29b-41d4-a716-446655440001",
  "documentId": "770e8400-e29b-41d4-a716-446655440002",
  "documentNumber": "DCN-2024-001",
  "signedXmlUrl": "http://localhost:9000/debitcreditnotes/770e8400-e29b-41d4-a716-446655440002.xml"
}
```

The service downloads signed XML from `signedXmlUrl` at runtime — no inline XML or JSON payload in the command.

### DocumentArchiveEvent Format

Published via outbox to `document.archive` after a successful generation:

| Field | Value |
|-------|-------|
| `documentId` | Debit/credit note document ID |
| `documentNumber` | Document number (e.g., `DCN-2024-001`) |
| `documentType` | `DEBIT_CREDIT_NOTE` |
| `artifactType` | `UNSIGNED_PDF` |
| `sourceUrl` | MinIO URL of the generated PDF |
| `fileName` | `DCN-2024-001.pdf` |
| `contentType` | `application/pdf` |
| `sagaId` | Saga orchestration instance ID |
| `correlationId` | End-to-end correlation ID |

## API Endpoints

This service is **event-driven only** — no business REST API. Only Spring Actuator endpoints are exposed:

| Endpoint | Description |
|----------|-------------|
| `GET /actuator/health` | Health check |
| `GET /actuator/metrics` | Micrometer metrics |
| `GET /actuator/prometheus` | Prometheus-format metrics |
| `GET /actuator/camelroutes` | Apache Camel route status |

## Project Structure

```
src/main/java/com/wpanther/debitcreditnote/pdf/
├── domain/
│   ├── model/              DebitCreditNotePdfDocument (aggregate root), GenerationStatus (enum)
│   ├── repository/          DebitCreditNotePdfDocumentRepository (domain interface)
│   ├── service/            DebitCreditNotePdfGenerationService (interface), PdfGenerationConstants
│   └── exception/          DebitCreditNotePdfGenerationException
├── application/
│   ├── port/in/            ProcessDebitCreditNotePdfUseCase, CompensateDebitCreditNotePdfUseCase
│   ├── port/out/           SagaReplyPort, PdfStoragePort, SignedXmlFetchPort, PdfEventPort, DocumentArchivePort
│   ├── dto/event/          DebitCreditNotePdfGeneratedEvent, DocumentArchiveEvent
│   └── service/            DebitCreditNotePdfDocumentService (orchestration)
└── infrastructure/
    ├── adapter/in/kafka/   SagaCommandHandler, SagaRouteConfig
    ├── adapter/out/
    │   ├── pdf/            FopDebitCreditNotePdfGenerator, DebitCreditNotePdfGenerationServiceImpl,
    │   │                   PdfA3Converter, ThaiAmountWordsConverter
    │   ├── storage/        MinioStorageAdapter, MinioCleanupService
    │   ├── persistence/    DebitCreditNotePdfDocumentEntity, JpaDebitCreditNotePdfDocumentRepository,
    │   │                   DebitCreditNotePdfDocumentRepositoryAdapter, outbox/
    │   ├── messaging/      SagaReplyPublisher, OutboxDocumentArchiveAdapter, EventPublisher
    │   └── client/         RestTemplateSignedXmlFetcher (fetches signed XML from MinIO)
    ├── config/             RestTemplateConfig, OutboxConfig, FontHealthCheck, MinioConfig
    └── metrics/            PdfGenerationMetrics
```

## Compensation Flow

On `saga.compensation.debit-credit-note-pdf`:

1. Find `DebitCreditNotePdfDocument` by `documentId`
2. Delete DB record
3. Delete PDF from MinIO (best-effort, log on failure)
4. Publish `COMPENSATED` reply to `saga.reply.debit-credit-note-pdf`

If deletion fails, publishes `COMPENSATION_FAILURE` (not `COMPENSATED`).

## Adding New Features

1. **Domain changes**: Add to `domain/model/` or `domain/service/`
2. **New use cases**: Create interface in `application/port/in/`
3. **New outbound dependency**: Define port in `application/port/out/` + implement in `infrastructure/adapter/out/`
4. **New adapter**: Implement in `infrastructure/adapter/`

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `signedXmlUrl is null or blank` | XML signing step did not produce a valid URL | Check `xml-signing-service` and MinIO bucket |
| `Circuit breaker open` | Too many consecutive failures calling MinIO or signed XML fetch | Check MinIO availability; wait for circuit to half-open |
| `Generated PDF exceeds max allowed size` | PDF larger than 50 MB default limit | Increase `app.pdf.generation.max-pdf-size-bytes` |
| Thai text renders incorrectly | Font files missing from classpath | Verify `fonts/THSarabunNew*.ttf` and `fonts/NotoSansThaiLooped*.ttf` are present |
| `Concurrent modification` exceptions | Optimistic locking conflict during retry | Handled automatically; saga orchestrator retries |

## License

MIT License

## Maintainer

**Weerachat Wongsawat** — rabbit_roger@yahoo.com