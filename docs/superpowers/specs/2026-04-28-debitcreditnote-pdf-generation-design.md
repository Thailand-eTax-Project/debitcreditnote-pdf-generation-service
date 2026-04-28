# Debit/Credit Note PDF Generation Service — Design Spec

**Date:** 2026-04-28  
**Status:** Approved  
**Scope:** `debitcreditnote-pdf-generation-service` (new) + `GENERATE_DEBIT_CREDIT_NOTE_PDF` SagaStep in `saga-commons`

---

## 1. Overview

A new Spring Boot microservice that generates PDF/A-3 documents for Thai e-Tax Debit/Credit Note (`DebitCreditNote_CrossIndustryInvoice`) documents. It participates in the Saga Orchestration pipeline, consuming commands from the orchestrator and replying via the transactional outbox pattern.

The service is a full hexagonal port of `taxinvoice-pdf-generation-service` (port 8089). Only the DebitCreditNote-specific namespaces, XSL-FO template, and class names differ. Both Debit Notes (`TypeCode=80`, `ใบเพิ่มหนี้`) and Credit Notes (`TypeCode=81`, `ใบลดหนี้`) use the same XML schema and are handled by this single service.

---

## 2. Saga Commons Change

**File:** `saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java`

Add one new enum value after `GENERATE_ABBREVIATED_TAX_INVOICE_PDF`:

```java
/**
 * Debit/Credit note PDF generation via debitcreditnote-pdf-generation-service.
 */
GENERATE_DEBIT_CREDIT_NOTE_PDF("generate-debit-credit-note-pdf", "Debit/Credit Note PDF Generation Service"),
```

---

## 3. Service Identity

| Property | Value |
|----------|-------|
| Port | `8097` |
| Artifact ID | `debitcreditnote-pdf-generation-service` |
| Package root | `com.wpanther.debitcreditnote.pdf` |
| Main class | `DebitCreditNotePdfGenerationServiceApplication` |
| Database | `debitcreditnotepdf_db` (PostgreSQL) |
| MinIO bucket | `debitcreditnotes` (env: `MINIO_BUCKET_NAME`) |
| Camel app name | `debitcreditnote-pdf-generation-camel` |

---

## 4. Architecture — Hexagonal (Port/Adapter)

```
com.wpanther.debitcreditnote.pdf/
├── domain/
│   ├── model/
│   │   ├── DebitCreditNotePdfDocument.java     # Aggregate root
│   │   └── GenerationStatus.java               # PENDING → GENERATING → COMPLETED/FAILED
│   ├── repository/
│   │   └── DebitCreditNotePdfDocumentRepository.java
│   ├── service/
│   │   └── DebitCreditNotePdfGenerationService.java  # Port interface
│   ├── exception/
│   │   └── DebitCreditNotePdfGenerationException.java
│   └── constants/
│       └── PdfGenerationConstants.java
│
├── application/
│   ├── service/
│   │   ├── SagaCommandHandler.java
│   │   └── DebitCreditNotePdfDocumentService.java
│   ├── usecase/
│   │   ├── ProcessDebitCreditNotePdfUseCase.java
│   │   └── CompensateDebitCreditNotePdfUseCase.java
│   └── port/out/
│       ├── PdfEventPort.java
│       ├── PdfStoragePort.java
│       ├── SagaReplyPort.java
│       └── SignedXmlFetchPort.java
│
└── infrastructure/
    ├── adapter/in/kafka/
    │   ├── KafkaDebitCreditNoteProcessCommand.java
    │   ├── KafkaDebitCreditNoteCompensateCommand.java
    │   ├── KafkaCommandMapper.java
    │   └── SagaRouteConfig.java
    ├── adapter/out/
    │   ├── client/
    │   │   └── RestTemplateSignedXmlFetcher.java
    │   ├── messaging/
    │   │   ├── EventPublisher.java
    │   │   ├── SagaReplyPublisher.java
    │   │   ├── DebitCreditNotePdfGeneratedEvent.java
    │   │   ├── DebitCreditNotePdfReplyEvent.java
    │   │   └── OutboxConstants.java
    │   ├── pdf/
    │   │   ├── FopDebitCreditNotePdfGenerator.java
    │   │   ├── PdfA3Converter.java
    │   │   ├── ThaiAmountWordsConverter.java
    │   │   └── DebitCreditNotePdfGenerationServiceImpl.java
    │   ├── persistence/
    │   │   ├── DebitCreditNotePdfDocumentEntity.java
    │   │   ├── JpaDebitCreditNotePdfDocumentRepository.java
    │   │   ├── DebitCreditNotePdfDocumentRepositoryAdapter.java
    │   │   └── outbox/
    │   │       ├── OutboxEventEntity.java
    │   │       ├── SpringDataOutboxRepository.java
    │   │       └── JpaOutboxEventRepository.java
    │   └── storage/
    │       ├── MinioStorageAdapter.java
    │       └── MinioCleanupService.java
    ├── config/
    │   ├── MinioConfig.java
    │   ├── OutboxConfig.java
    │   ├── RestTemplateConfig.java
    │   └── FontHealthCheck.java
    └── metrics/
        └── PdfGenerationMetrics.java
```

---

## 5. Kafka Topics

| Topic | Direction | Description |
|-------|-----------|-------------|
| `saga.command.debit-credit-note-pdf` | Consume | Process command from Orchestrator |
| `saga.compensation.debit-credit-note-pdf` | Consume | Compensation command from Orchestrator |
| `pdf.generated.debit-credit-note` | Produce (outbox) | Notification Service |
| `saga.reply.debit-credit-note-pdf` | Produce (outbox) | Reply to Orchestrator |
| `pdf.generation.debit-credit-note.dlq` | Produce | Dead letter queue |

Consumer group IDs:
- Command: `debitcreditnote-pdf-generation-command`
- Compensation: `debitcreditnote-pdf-generation-compensation`

---

## 6. Saga Command/Reply Schema

### Input: `KafkaDebitCreditNoteProcessCommand` (extends `SagaCommand`)

```json
{
  "eventId": "uuid",
  "occurredAt": "...",
  "eventType": "...",
  "version": 1,
  "sagaId": "uuid",
  "sagaStep": "generate-debit-credit-note-pdf",
  "correlationId": "uuid",
  "documentId": "uuid",
  "documentNumber": "DCN-2024-001",
  "signedXmlUrl": "http://document-storage/documents/..."
}
```

### Output: `DebitCreditNotePdfReplyEvent` (extends `SagaReply`)

```json
{
  "sagaId": "uuid",
  "sagaStep": "generate-debit-credit-note-pdf",
  "correlationId": "uuid",
  "status": "SUCCESS|FAILURE|COMPENSATED",
  "errorMessage": null,
  "pdfUrl": "http://localhost:9000/debitcreditnotes/2024/01/15/debitcreditnote-DCN-2024-001-abc.pdf",
  "pdfSize": 12345
}
```

`pdfUrl` and `pdfSize` are present only on SUCCESS replies. The Orchestrator stores them in `DocumentMetadata` for the subsequent `PDF_STORAGE` step.

### Output: `DebitCreditNotePdfGeneratedEvent` (outbox → `pdf.generated.debit-credit-note`)

```json
{
  "eventId": "uuid",
  "eventType": "pdf.generated.debit-credit-note",
  "version": 1,
  "documentId": "uuid",
  "debitCreditNoteId": "uuid",
  "documentNumber": "DCN-2024-001",
  "documentUrl": "http://...",
  "fileSize": 12345,
  "xmlEmbedded": true,
  "correlationId": "uuid"
}
```

---

## 7. Domain Model — `DebitCreditNotePdfDocument`

Aggregate root with the same state machine as `TaxInvoicePdfDocument`:

| State transition | Method | Guard |
|-----------------|--------|-------|
| `PENDING → GENERATING` | `startGeneration()` | Must be PENDING |
| `GENERATING → COMPLETED` | `markCompleted(path, url, size)` | Must be GENERATING |
| `any → FAILED` | `markFailed(message)` | — |

Key fields: `debitCreditNoteId` (unique constraint / idempotency key), `documentNumber`, `documentPath` (MinIO S3 key), `documentUrl` (full MinIO URL), `retryCount` (max 3).

MinIO S3 key pattern: `YYYY/MM/DD/debitcreditnote-{documentNumber}-{uuid}.pdf`

---

## 8. PDF Generation Pipeline

```
KafkaDebitCreditNoteProcessCommand
        ↓
SagaCommandHandler
        ├── Idempotency check (COMPLETED? re-publish and return SUCCESS)
        ├── Retry limit check (retryCount >= maxRetries? send FAILURE)
        └── DebitCreditNotePdfDocumentService.generatePdf()
                ├── Create domain aggregate (PENDING → GENERATING)
                ├── DebitCreditNotePdfGenerationServiceImpl.generatePdf()
                │   ├── RestTemplateSignedXmlFetcher.fetch(signedXmlUrl) → signedXml
                │   ├── XPath on signedXml (DebitCreditNote namespaces)
                │   │     → extract GrandTotalAmount
                │   ├── ThaiAmountWordsConverter.toWords(grandTotal) → amountInWords
                │   ├── FopDebitCreditNotePdfGenerator (amountInWords as XSLT param) → base PDF
                │   └── PdfA3Converter → PDF/A-3b with embedded XML
                ├── MinioStorageAdapter.store(pdf, key) → pdfUrl
                └── markCompleted() → COMPLETED
        ├── EventPublisher → outbox_events (pdf.generated.debit-credit-note)
        └── SagaReplyPublisher → outbox_events (saga.reply.debit-credit-note-pdf)
```

### Compensation Flow

```
KafkaDebitCreditNoteCompensateCommand
        ↓
SagaCommandHandler.handleCompensation()
        ├── MinioStorageAdapter.delete(documentPath)
        ├── Delete database record
        └── SagaReplyPublisher → COMPENSATED reply (idempotent if no record)
```

---

## 9. XSL-FO Template (`debitcreditnote-direct.xsl`)

Adapted from `taxinvoice-direct.xsl`. Structural changes:

| Element | TaxInvoice | DebitCreditNote |
|---------|-----------|-----------------|
| `rsm` namespace URI | `...TaxInvoice_CrossIndustryInvoice:2` | `...DebitCreditNote_CrossIndustryInvoice:2` |
| `ram` namespace URI | `...TaxInvoice_ReusableAggregateBusinessInformationEntity:2` | `...DebitCreditNote_ReusableAggregateBusinessInformationEntity:2` |
| Root match | `/rsm:TaxInvoice_CrossIndustryInvoice` | `/rsm:DebitCreditNote_CrossIndustryInvoice` |
| Document title (Thai) | `ใบเสร็จรับเงิน/ใบกำกับภาษี` | `<xsl:value-of select="$doc/ram:Name"/>` (dynamic from XML) |

### Debit/Credit-specific layout additions

**Header section** (near document ID and issue date) — rendered with `<xsl:if test="...">` so missing values are silently skipped:

| Label | XPath |
|-------|-------|
| เหตุผล (Purpose) | `$doc/ram:Purpose` |
| รหัสเหตุผล (Purpose Code) | `$doc/ram:PurposeCode` |

**Monetary summary table** (additional rows alongside `GrandTotalAmount`) — also wrapped in `<xsl:if>`:

| Row label | XPath |
|-----------|-------|
| ยอดเงินตามเอกสารเดิม (Original Amount) | `$summation/ram:OriginalInformationAmount` |
| ผลต่าง (Difference Amount) | `$summation/ram:DifferenceInformationAmount` |

Resources copied as-is from taxinvoice: `fop.xconf`, `sRGB.icc`, Thai font files (`THSarabunNew`, `NotoSansThaiLooped`).

---

## 10. Database — `debitcreditnotepdf_db`

Single Flyway migration: `V1__create_debit_credit_note_pdf_tables.sql`

Creates in one script:
- `debit_credit_note_pdf_documents` table with `debit_credit_note_id` unique constraint and `retry_count` column
- `outbox_events` table with compound index on `(status, created_at)`

---

## 11. Configuration (`application.yml`)

All env vars mirror taxinvoice defaults. DebitCreditNote-specific values:

| Variable | Default |
|----------|---------|
| `server.port` | `8097` |
| `DB_NAME` | `debitcreditnotepdf_db` |
| `MINIO_BUCKET_NAME` | `debitcreditnotes` |
| `KAFKA_COMMAND_GROUP_ID` | `debitcreditnote-pdf-generation-command` |
| `KAFKA_COMPENSATION_GROUP_ID` | `debitcreditnote-pdf-generation-compensation` |

All other variables (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `KAFKA_BROKERS`, `PDF_GENERATION_MAX_RETRIES`, `PDF_MAX_CONCURRENT_RENDERS`, `PDF_MAX_SIZE_BYTES`, `REST_CLIENT_CONNECT_TIMEOUT`, `REST_CLIENT_READ_TIMEOUT`, `FONT_HEALTH_CHECK_ENABLED`, etc.) use the same defaults as taxinvoice-pdf-generation-service.

---

## 12. Testing Strategy

90% JaCoCo line coverage requirement. H2 in-memory DB, Flyway disabled in `application-test.yml`. Simplified `fop.xconf` for tests (no PDF/A mode, auto-detect fonts — no Thai font files required).

| Test class | What it covers |
|------------|---------------|
| `SagaCommandHandlerTest` | success, idempotency, max retries, generation failure, compensation success, idempotent compensation, compensation failure |
| `CamelRouteConfigTest` | JSON serialization/deserialization of all event types |
| `FopDebitCreditNotePdfGeneratorTest` | constructor validation, semaphore, valid/malformed XML, size limit, thread interruption, URI resolution, font availability, dynamic title from `ram:Name`, Purpose/PurposeCode rendering, OriginalInformationAmount/DifferenceInformationAmount rendering |
| `PdfA3ConverterTest` | constructor, null/empty PDF, exception constructors |
| `MinioStorageAdapterTest` | upload, delete, URL resolution, Thai chars, filename sanitization |
| `DebitCreditNotePdfDocumentTest` | state machine transitions, invariants, retry counting |
| `DebitCreditNotePdfDocumentServiceTest` | transactional service methods |
| `EventPublisherTest` | outbox event publishing |
| `SagaReplyPublisherTest` | outbox reply publishing |
| `RestTemplateSignedXmlFetcherTest` | REST client with circuit breaker |
| `KafkaCommandMapperTest` | command mapping |
| `MinioCleanupServiceTest` | cleanup scheduling |
| `FontHealthCheckTest` | font validation at startup |

No embedded Kafka integration tests. No REST API — Spring Actuator only (`/actuator/health`, `/actuator/metrics`, `/actuator/camelroutes`, `/actuator/prometheus`).

---

## 13. Out of Scope

- Orchestrator changes (routing `GENERATE_DEBIT_CREDIT_NOTE_PDF` commands) — separate task
- `debitcreditnote-processing-service` changes
- Integration tests with embedded Kafka
