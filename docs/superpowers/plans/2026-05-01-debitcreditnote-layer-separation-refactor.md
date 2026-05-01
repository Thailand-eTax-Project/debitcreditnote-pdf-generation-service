# debitcreditnote-pdf-generation-service Layer Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor debitcreditnote-pdf-generation-service to proper layer separation: saga infrastructure types (SagaCommand + Jackson DTOs) in `infrastructure/adapter/in/kafka/dto/`, plain-parameter use case interfaces in `application/port/in/`, SagaCommandHandler as a driving adapter in `infrastructure/adapter/in/kafka/`, and DebitCreditNotePdfReplyEvent factory inlined in SagaReplyPublisher.

**Architecture:** Kafka DTOs (extends SagaCommand + Jackson) live in `infrastructure/adapter/in/kafka/dto/`. Use case interfaces accept plain fields. SagaCommandHandler routes DTOs to use cases by extracting fields — no command objects flow into application/domain layers. Notification events (TraceEvent subclasses) go to `application/dto/event/`.

**Tech Stack:** Java 21, Spring Boot 3.2.5, Apache Camel 4.14.4, saga-commons library, Jackson

---

## Before You Start

- Build the service to confirm it compiles: `mvn clean compile -q`
- Run tests to confirm baseline: `mvn clean test -q`
- Work inside the service directory: `/home/wpanther/projects/etax/invoice-microservices/services/debitcreditnote-pdf-generation-service`

---

## File Changes Overview

```
CREATING:
  infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCommand.java             (rename from KafkaDebitCreditNoteProcessCommand)
  infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCompensationCommand.java  (rename from KafkaDebitCreditNoteCompensateCommand)
  application/port/in/ProcessDebitCreditNotePdfUseCase.java                       (plain params)
  application/port/in/CompensateDebitCreditNotePdfUseCase.java                    (plain params)

MOVING:
  SagaCommandHandler.java          application/service/ → infrastructure/adapter/in/kafka/
  application/event/DebitCreditNotePdfGeneratedEvent.java → application/dto/event/DebitCreditNotePdfGeneratedEvent.java

DELETING:
  infrastructure/adapter/in/kafka/KafkaCommandMapper.java
  infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java
  infrastructure/adapter/in/kafka/KafkaDebitCreditNoteProcessCommand.java
  infrastructure/adapter/in/kafka/KafkaDebitCreditNoteCompensateCommand.java
  infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent.java
  application/event/DebitCreditNotePdfGeneratedEvent.java (original)
  application/service/SagaCommandHandler.java (original location)
  application/usecase/ProcessDebitCreditNotePdfUseCase.java
  application/usecase/CompensateDebitCreditNotePdfUseCase.java

MODIFYING:
  DebitCreditNotePdfDocumentService.java  (method signatures change from command objects to plain fields)
  SagaRouteConfig.java                     (use new DTO package, remove KafkaCommandMapper, call use cases with plain params)
  SagaReplyPublisher.java                  (inline DebitCreditNotePdfReplyEvent factory)
  EventPublisher.java                      (import new package path for DebitCreditNotePdfGeneratedEvent)
```

---

## Task 1: Create `dto/` directory and new `DebitCreditNotePdfCommand`

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCommand.java`
- Source: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteProcessCommand.java`

- [ ] **Step 1: Create the directory and new DTO**

```bash
mkdir -p src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/dto
```

Create `DebitCreditNotePdfCommand.java` — removes the `implements ProcessDebitCreditNotePdfUseCase.Command` from the existing Kafka command:

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Getter
public class DebitCreditNotePdfCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @JsonProperty("documentId")
    private final String documentId;

    @JsonProperty("documentNumber")
    private final String documentNumber;

    @JsonProperty("signedXmlUrl")
    private final String signedXmlUrl;

    @JsonCreator
    public DebitCreditNotePdfCommand(
            @JsonProperty("eventId")        UUID eventId,
            @JsonProperty("occurredAt")     Instant occurredAt,
            @JsonProperty("eventType")      String eventType,
            @JsonProperty("version")        int version,
            @JsonProperty("sagaId")         String sagaId,
            @JsonProperty("sagaStep")       SagaStep sagaStep,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("documentId")     String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("signedXmlUrl")   String signedXmlUrl) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId     = Objects.requireNonNull(documentId, "documentId is required");
        this.documentNumber = Objects.requireNonNull(documentNumber, "documentNumber is required");
        this.signedXmlUrl   = Objects.requireNonNull(signedXmlUrl, "signedXmlUrl is required");
    }

    /** Convenience constructor for testing. */
    public DebitCreditNotePdfCommand(String sagaId, SagaStep sagaStep, String correlationId,
                                     String documentId, String documentNumber, String signedXmlUrl) {
        super(sagaId, sagaStep, correlationId);
        this.documentId     = Objects.requireNonNull(documentId, "documentId is required");
        this.documentNumber = Objects.requireNonNull(documentNumber, "documentNumber is required");
        this.signedXmlUrl   = Objects.requireNonNull(signedXmlUrl, "signedXmlUrl is required");
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors related to the new file

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCommand.java
git commit -m "refactor: rename KafkaDebitCreditNoteProcessCommand to DebitCreditNotePdfCommand in dto/ package"
```

---

## Task 2: Create `DebitCreditNotePdfCompensationCommand` in `dto/`

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCompensationCommand.java`
- Source: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteCompensateCommand.java`

- [ ] **Step 1: Create the new DTO**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Getter
public class DebitCreditNotePdfCompensationCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @JsonProperty("documentId")
    private final String documentId;

    @JsonCreator
    public DebitCreditNotePdfCompensationCommand(
            @JsonProperty("eventId")       UUID eventId,
            @JsonProperty("occurredAt")    Instant occurredAt,
            @JsonProperty("eventType")     String eventType,
            @JsonProperty("version")       int version,
            @JsonProperty("sagaId")        String sagaId,
            @JsonProperty("sagaStep")      SagaStep sagaStep,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("documentId")    String documentId) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId = Objects.requireNonNull(documentId, "documentId is required");
    }

    /** Convenience constructor for testing. */
    public DebitCreditNotePdfCompensationCommand(String sagaId, SagaStep sagaStep,
                                                  String correlationId, String documentId) {
        super(sagaId, sagaStep, correlationId);
        this.documentId = Objects.requireNonNull(documentId, "documentId is required");
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCompensationCommand.java
git commit -m "refactor: rename KafkaDebitCreditNoteCompensateCommand to DebitCreditNotePdfCompensationCommand in dto/ package"
```

---

## Task 3: Create `ProcessDebitCreditNotePdfUseCase` in `application/port/in/`

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/in/ProcessDebitCreditNotePdfUseCase.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/ProcessDebitCreditNotePdfUseCase.java` (later in Task 9)

- [ ] **Step 1: Create the directory and new interface**

```bash
mkdir -p src/main/java/com/wpanther/debitcreditnote/pdf/application/port/in
```

```java
package com.wpanther.debitcreditnote.pdf.application.port.in;

import com.wpanther.saga.domain.enums.SagaStep;

/**
 * Inbound port for debit/credit note PDF generation.
 * Called by SagaCommandHandler with plain fields — no command objects.
 */
public interface ProcessDebitCreditNotePdfUseCase {

    void handle(String documentId, String documentNumber, String signedXmlUrl,
                String sagaId, SagaStep sagaStep, String correlationId);
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/application/port/in/ProcessDebitCreditNotePdfUseCase.java
git commit -m "refactor: add ProcessDebitCreditNotePdfUseCase in application/port/in/ with plain parameter signatures"
```

---

## Task 4: Create `CompensateDebitCreditNotePdfUseCase` in `application/port/in/`

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/in/CompensateDebitCreditNotePdfUseCase.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/CompensateDebitCreditNotePdfUseCase.java` (later in Task 9)

- [ ] **Step 1: Create the new interface**

```java
package com.wpanther.debitcreditnote.pdf.application.port.in;

import com.wpanther.saga.domain.enums.SagaStep;

/**
 * Inbound port for debit/credit note PDF compensation.
 * Called by SagaCommandHandler with plain fields — no command objects.
 */
public interface CompensateDebitCreditNotePdfUseCase {

    void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId);
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/application/port/in/CompensateDebitCreditNotePdfUseCase.java
git commit -m "refactor: add CompensateDebitCreditNotePdfUseCase in application/port/in/ with plain parameter signatures"
```

---

## Task 5: Rewrite `SagaCommandHandler` in `infrastructure/adapter/in/kafka/`

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/SagaCommandHandler.java` (new location)
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandler.java` (later in Task 10)
- Modify: `DebitCreditNotePdfDocumentService.java` (Task 7)

The handler implements the plain-parameter use case interfaces and extracts fields from DTOs before calling application services. Also fixes the compensation bug where `publishCompensated` was called even when deletion failed.

- [ ] **Step 1: Write the new SagaCommandHandler**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.debitcreditnote.pdf.application.port.in.CompensateDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.application.port.in.ProcessDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.application.service.DebitCreditNotePdfDocumentService;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfStoragePort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.service.DebitCreditNotePdfGenerationService;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCompensationCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCommand;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;

import java.util.Optional;

/**
 * Saga command handler — driving adapter that receives Kafka messages and calls use cases.
 * No command objects flow into domain or application layers — only plain field parameters.
 */
@Service
@Slf4j
public class SagaCommandHandler implements ProcessDebitCreditNotePdfUseCase, CompensateDebitCreditNotePdfUseCase {

    private static final String MDC_SAGA_ID         = "sagaId";
    private static final String MDC_CORRELATION_ID  = "correlationId";
    private static final String MDC_DOCUMENT_NUMBER = "documentNumber";
    private static final String MDC_DOCUMENT_ID     = "documentId";

    private final DebitCreditNotePdfDocumentService pdfDocumentService;
    private final DebitCreditNotePdfGenerationService pdfGenerationService;
    private final PdfStoragePort pdfStoragePort;
    private final SagaReplyPort sagaReplyPort;
    private final SignedXmlFetchPort signedXmlFetchPort;
    private final int maxRetries;

    public SagaCommandHandler(DebitCreditNotePdfDocumentService pdfDocumentService,
                              DebitCreditNotePdfGenerationService pdfGenerationService,
                              PdfStoragePort pdfStoragePort,
                              SagaReplyPort sagaReplyPort,
                              SignedXmlFetchPort signedXmlFetchPort,
                              @Value("${app.pdf.generation.max-retries:3}") int maxRetries) {
        this.pdfDocumentService  = pdfDocumentService;
        this.pdfGenerationService = pdfGenerationService;
        this.pdfStoragePort      = pdfStoragePort;
        this.sagaReplyPort       = sagaReplyPort;
        this.signedXmlFetchPort  = signedXmlFetchPort;
        this.maxRetries          = maxRetries;
    }

    @Override
    public void handle(String documentId, String documentNumber, String signedXmlUrl,
                       String sagaId, SagaStep sagaStep, String correlationId) {
        MDC.put(MDC_SAGA_ID,         sagaId);
        MDC.put(MDC_CORRELATION_ID,  correlationId);
        MDC.put(MDC_DOCUMENT_NUMBER, documentNumber);
        MDC.put(MDC_DOCUMENT_ID,     documentId);
        try {
            log.info("Handling ProcessCommand for saga {} document {}", sagaId, documentNumber);
            try {
                if (signedXmlUrl == null || signedXmlUrl.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(sagaId, sagaStep, correlationId, "signedXmlUrl is null or blank");
                    return;
                }
                if (documentId == null || documentId.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(sagaId, sagaStep, correlationId, "documentId is null or blank");
                    return;
                }
                if (documentNumber == null || documentNumber.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(sagaId, sagaStep, correlationId, "documentNumber is null or blank");
                    return;
                }

                Optional<DebitCreditNotePdfDocument> existing =
                        pdfDocumentService.findByDebitCreditNoteId(documentId);

                if (existing.isPresent() && existing.get().isCompleted()) {
                    pdfDocumentService.publishIdempotentSuccess(existing.get(), sagaId, sagaStep, correlationId, documentId, documentNumber);
                    return;
                }

                int previousRetryCount = existing.map(DebitCreditNotePdfDocument::getRetryCount).orElse(-1);

                if (existing.isPresent() && existing.get().isMaxRetriesExceeded(maxRetries)) {
                    pdfDocumentService.publishRetryExhausted(sagaId, sagaStep, correlationId, documentId, documentNumber);
                    return;
                }

                DebitCreditNotePdfDocument document;
                if (existing.isPresent()) {
                    document = pdfDocumentService.replaceAndBeginGeneration(
                            existing.get().getId(), previousRetryCount, documentId, documentNumber);
                } else {
                    document = pdfDocumentService.beginGeneration(documentId, documentNumber);
                }

                String s3Key = null;
                try {
                    String signedXml = signedXmlFetchPort.fetch(signedXmlUrl);
                    byte[] pdfBytes  = pdfGenerationService.generatePdf(documentNumber, signedXml);
                    s3Key = pdfStoragePort.store(documentNumber, pdfBytes);
                    String fileUrl   = pdfStoragePort.resolveUrl(s3Key);

                    pdfDocumentService.completeGenerationAndPublish(
                            document.getId(), s3Key, fileUrl, pdfBytes.length, previousRetryCount,
                            sagaId, sagaStep, correlationId, documentId, documentNumber);

                } catch (CallNotPermittedException e) {
                    log.warn("Circuit breaker OPEN for saga {} document {}: {}", sagaId, documentNumber, e.getMessage());
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), "Circuit breaker open: " + e.getMessage(),
                            previousRetryCount, sagaId, sagaStep, correlationId);

                } catch (RestClientException e) {
                    log.warn("HTTP error fetching signed XML for saga {} document {}: {}", sagaId, documentNumber, e.getMessage());
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), "HTTP error fetching signed XML: " + describeThrowable(e),
                            previousRetryCount, sagaId, sagaStep, correlationId);

                } catch (Exception e) {
                    if (s3Key != null) {
                        try { pdfStoragePort.delete(s3Key); }
                        catch (Exception del) {
                            log.error("[ORPHAN_PDF] s3Key={} saga={} error={}", s3Key, sagaId, describeThrowable(del));
                        }
                    }
                    log.error("PDF generation failed for saga {} document {}: {}", sagaId, documentNumber, e.getMessage(), e);
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), describeThrowable(e), previousRetryCount, sagaId, sagaStep, correlationId);
                }

            } catch (OptimisticLockingFailureException e) {
                log.warn("Concurrent modification for saga {}: {}", sagaId, e.getMessage());
                pdfDocumentService.publishGenerationFailure(sagaId, sagaStep, correlationId, "Concurrent modification: " + e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error for saga {}: {}", sagaId, e.getMessage(), e);
                pdfDocumentService.publishGenerationFailure(sagaId, sagaStep, correlationId, describeThrowable(e));
            }
        } finally {
            MDC.clear();
        }
    }

    @Override
    public void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId) {
        MDC.put(MDC_SAGA_ID,        sagaId);
        MDC.put(MDC_CORRELATION_ID,  correlationId);
        MDC.put(MDC_DOCUMENT_ID,     documentId);
        try {
            log.info("Handling compensation for saga {} document {}", sagaId, documentId);
            try {
                Optional<DebitCreditNotePdfDocument> existing =
                        pdfDocumentService.findByDebitCreditNoteId(documentId);
                if (existing.isPresent()) {
                    DebitCreditNotePdfDocument doc = existing.get();
                    pdfDocumentService.deleteById(doc.getId());
                    if (doc.getDocumentPath() != null) {
                        try { pdfStoragePort.delete(doc.getDocumentPath()); }
                        catch (Exception e) {
                            log.warn("Failed to delete PDF from MinIO for saga {} key {}: {}",
                                    sagaId, doc.getDocumentPath(), e.getMessage());
                        }
                    }
                    log.info("Compensated document {} for saga {}", doc.getId(), sagaId);
                } else {
                    log.info("No document for documentId {} — already compensated", documentId);
                }
                pdfDocumentService.publishCompensated(sagaId, sagaStep, correlationId);
            } catch (Exception e) {
                log.error("Failed to compensate for saga {}: {}", sagaId, e.getMessage(), e);
                pdfDocumentService.publishCompensationFailure(sagaId, sagaStep, correlationId, "Compensation failed: " + describeThrowable(e));
            }
        } finally {
            MDC.clear();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailure(SagaCommand command, Throwable cause) {
        try {
            sagaReplyPort.publishFailure(command.getSagaId(), command.getSagaStep(),
                    command.getCorrelationId(),
                    "Message routed to DLQ after retry exhaustion: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of DLQ failure for saga {}", command.getSagaId(), e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishCompensationOrchestrationFailure(SagaCommand command, Throwable cause) {
        try {
            sagaReplyPort.publishFailure(command.getSagaId(), command.getSagaStep(),
                    command.getCorrelationId(),
                    "Compensation DLQ after retry exhaustion: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of compensation DLQ failure for saga {}", command.getSagaId(), e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailureForUnparsedMessage(
            String sagaId, SagaStep sagaStep, String correlationId, Throwable cause) {
        try {
            sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId,
                    "Message routed to DLQ after deserialization failure: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of DLQ deserialization failure for saga {} — orchestrator must timeout",
                    sagaId, e);
        }
    }

    private String describeThrowable(Throwable t) {
        if (t == null) return "unknown error";
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg != null ? ": " + msg : "");
    }
}
```

- [ ] **Step 2: Compile and fix any errors**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Compile errors in `DebitCreditNotePdfDocumentService` (method signatures don't match yet) — this is expected

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/SagaCommandHandler.java
git commit -m "refactor: move SagaCommandHandler to infrastructure/adapter/in/kafka/ with plain parameter calls"
```

---

## Task 6: Move `DebitCreditNotePdfGeneratedEvent` to `application/dto/event/`

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/dto/event/DebitCreditNotePdfGeneratedEvent.java`
- Modify: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/EventPublisher.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/event/DebitCreditNotePdfGeneratedEvent.java` (original, after verifying)

- [ ] **Step 1: Create the directory and new file**

```bash
mkdir -p src/main/java/com/wpanther/debitcreditnote/pdf/application/dto/event
```

```java
package com.wpanther.debitcreditnote.pdf.application.dto.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.model.TraceEvent;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Getter
public class DebitCreditNotePdfGeneratedEvent extends TraceEvent {

    private static final String EVENT_TYPE  = "pdf.generated.debit-credit-note";
    private static final String SOURCE      = "debitcreditnote-pdf-generation-service";
    private static final String TRACE_TYPE  = "PDF_GENERATED";

    @JsonProperty("documentId")     private final String documentId;
    @JsonProperty("documentNumber") private final String documentNumber;
    @JsonProperty("documentUrl")    private final String documentUrl;
    @JsonProperty("fileSize")       private final long fileSize;
    @JsonProperty("xmlEmbedded")    private final boolean xmlEmbedded;

    public DebitCreditNotePdfGeneratedEvent(
            String sagaId, String documentId, String documentNumber,
            String documentUrl, long fileSize, boolean xmlEmbedded, String correlationId) {
        super(sagaId, correlationId, SOURCE, TRACE_TYPE, null);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.documentUrl    = documentUrl;
        this.fileSize       = fileSize;
        this.xmlEmbedded    = xmlEmbedded;
    }

    @Override
    public String getEventType() { return EVENT_TYPE; }

    @JsonCreator
    public DebitCreditNotePdfGeneratedEvent(
            @JsonProperty("eventId")        UUID eventId,
            @JsonProperty("occurredAt")     Instant occurredAt,
            @JsonProperty("eventType")      String eventType,
            @JsonProperty("version")        int version,
            @JsonProperty("sagaId")         String sagaId,
            @JsonProperty("correlationId")  String correlationId,
            @JsonProperty("source")         String source,
            @JsonProperty("traceType")      String traceType,
            @JsonProperty("context")        String context,
            @JsonProperty("documentId")     String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("documentUrl")    String documentUrl,
            @JsonProperty("fileSize")       long fileSize,
            @JsonProperty("xmlEmbedded")    boolean xmlEmbedded) {
        super(eventId, occurredAt, eventType, version, sagaId, correlationId, source, traceType, context);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.documentUrl    = documentUrl;
        this.fileSize       = fileSize;
        this.xmlEmbedded    = xmlEmbedded;
    }
}
```

- [ ] **Step 2: Update EventPublisher import**

In `EventPublisher.java`, change the import from:
```java
import com.wpanther.debitcreditnote.pdf.application.event.DebitCreditNotePdfGeneratedEvent;
```
to:
```java
import com.wpanther.debitcreditnote.pdf.application.dto.event.DebitCreditNotePdfGeneratedEvent;
```

- [ ] **Step 3: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Errors in `DebitCreditNotePdfDocumentService` (will fix in Task 7)

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/application/dto/event/DebitCreditNotePdfGeneratedEvent.java
git add src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/EventPublisher.java
git commit -m "refactor: move DebitCreditNotePdfGeneratedEvent to application/dto/event/"
```

---

## Task 7: Update `DebitCreditNotePdfDocumentService` method signatures

**Files:**
- Modify: `src/main/java/com/wpanther/debitcreditnote/pdf/application/service/DebitCreditNotePdfDocumentService.java`

All methods that took command objects now take individual field parameters.

- [ ] **Step 1: Rewrite the service with plain parameter signatures**

Replace the entire `DebitCreditNotePdfDocumentService.java` with this:

```java
package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.dto.event.DebitCreditNotePdfGeneratedEvent;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.metrics.PdfGenerationMetrics;
import com.wpanther.saga.domain.enums.SagaStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DebitCreditNotePdfDocumentService {

    private final DebitCreditNotePdfDocumentRepository repository;
    private final PdfEventPort pdfEventPort;
    private final SagaReplyPort sagaReplyPort;
    private final PdfGenerationMetrics pdfGenerationMetrics;

    @Transactional(readOnly = true)
    public Optional<DebitCreditNotePdfDocument> findByDebitCreditNoteId(String debitCreditNoteId) {
        return repository.findByDebitCreditNoteId(debitCreditNoteId);
    }

    @Transactional
    public DebitCreditNotePdfDocument beginGeneration(String debitCreditNoteId, String documentNumber) {
        log.info("Initiating PDF generation for debit/credit note: {}", documentNumber);
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(debitCreditNoteId)
                .documentNumber(documentNumber)
                .build();
        doc.startGeneration();
        return repository.save(doc);
    }

    @Transactional
    public DebitCreditNotePdfDocument replaceAndBeginGeneration(
            UUID existingId, int previousRetryCount, String debitCreditNoteId, String documentNumber) {
        log.info("Replacing document {} and re-starting generation for: {}", existingId, documentNumber);
        repository.deleteById(existingId);
        repository.flush();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(debitCreditNoteId)
                .documentNumber(documentNumber)
                .build();
        doc.startGeneration();
        doc.incrementRetryCountTo(previousRetryCount + 1);
        return repository.save(doc);
    }

    @Transactional
    public void completeGenerationAndPublish(UUID documentId, String s3Key, String fileUrl,
                                             long fileSize, int previousRetryCount,
                                             String sagaId, SagaStep sagaStep, String correlationId,
                                             String documentId, String documentNumber) {
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markCompleted(s3Key, fileUrl, fileSize);
        doc.markXmlEmbedded();
        applyRetryCount(doc, previousRetryCount);
        doc = repository.save(doc);

        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(doc, sagaId, correlationId, documentId, documentNumber));
        sagaReplyPort.publishSuccess(sagaId, sagaStep, correlationId, doc.getDocumentUrl(), doc.getFileSize());

        log.info("Completed PDF generation for saga {} debit/credit note {}", sagaId, doc.getDocumentNumber());
    }

    @Transactional
    public void failGenerationAndPublish(UUID documentId, String errorMessage,
                                         int previousRetryCount,
                                         String sagaId, SagaStep sagaStep, String correlationId) {
        String safeError = errorMessage != null ? errorMessage : "PDF generation failed";
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markFailed(safeError);
        applyRetryCount(doc, previousRetryCount);
        repository.save(doc);
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, safeError);
        log.warn("PDF generation failed for saga {} debit/credit note {}: {}", sagaId, doc.getDocumentNumber(), safeError);
    }

    @Transactional
    public void deleteById(UUID documentId) {
        repository.deleteById(documentId);
        repository.flush();
    }

    @Transactional
    public void publishIdempotentSuccess(DebitCreditNotePdfDocument existing,
                                         String sagaId, SagaStep sagaStep, String correlationId,
                                         String documentId, String documentNumber) {
        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(existing, sagaId, correlationId, documentId, documentNumber));
        sagaReplyPort.publishSuccess(sagaId, sagaStep, correlationId, existing.getDocumentUrl(), existing.getFileSize());
        log.warn("Debit/credit note PDF already generated for saga {} — re-publishing SUCCESS reply", sagaId);
    }

    @Transactional
    public void publishRetryExhausted(String sagaId, SagaStep sagaStep, String correlationId,
                                     String documentId, String documentNumber) {
        pdfGenerationMetrics.recordRetryExhausted(sagaId, documentId, documentNumber);
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, "Maximum retry attempts exceeded");
        log.error("Max retries exceeded for saga {} document {}", sagaId, documentNumber);
    }

    @Transactional
    public void publishGenerationFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, errorMessage);
    }

    @Transactional
    public void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId) {
        sagaReplyPort.publishCompensated(sagaId, sagaStep, correlationId);
    }

    @Transactional
    public void publishCompensationFailure(String sagaId, SagaStep sagaStep, String correlationId, String error) {
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, error);
    }

    private DebitCreditNotePdfDocument requireDocument(UUID documentId) {
        return repository.findById(documentId)
                .orElseThrow(() -> new IllegalStateException(
                        "Expected debit/credit note PDF document is absent: " + documentId));
    }

    private void applyRetryCount(DebitCreditNotePdfDocument doc, int previousRetryCount) {
        if (previousRetryCount < 0) return;
        doc.incrementRetryCountTo(previousRetryCount + 1);
    }

    private DebitCreditNotePdfGeneratedEvent buildGeneratedEvent(DebitCreditNotePdfDocument doc,
                                                                  String sagaId, String correlationId,
                                                                  String documentId, String documentNumber) {
        return new DebitCreditNotePdfGeneratedEvent(
                sagaId, documentId, doc.getDocumentNumber(),
                doc.getDocumentUrl(), doc.getFileSize(), doc.isXmlEmbedded(), correlationId);
    }
}
```

**Note:** `completeGenerationAndPublish` has a parameter name conflict — the parameter `documentId` shadows the method parameter `documentId`. Rename in the method signature to avoid the conflict:
```java
public void completeGenerationAndPublish(UUID id, String s3Key, String fileUrl,
                                         long fileSize, int previousRetryCount,
                                         String sagaId, SagaStep sagaStep, String correlationId,
                                         String docId, String docNumber) {
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Should compile now

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/application/service/DebitCreditNotePdfDocumentService.java
git commit -m "refactor: update DebitCreditNotePdfDocumentService method signatures to use plain fields"
```

---

## Task 8: Inline `DebitCreditNotePdfReplyEvent` factory in `SagaReplyPublisher`

**Files:**
- Modify: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent.java` (later in Task 9)

- [ ] **Step 1: Rewrite SagaReplyPublisher to inline the reply event factory**

Replace the entire `SagaReplyPublisher.java` with this:

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaReply;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaReplyPublisher implements SagaReplyPort {

    private static final String REPLY_TOPIC    = "saga.reply.debit-credit-note-pdf";
    private static final String AGGREGATE_TYPE = OutboxConstants.AGGREGATE_TYPE;

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId,
                             String pdfUrl, long pdfSize) {
        DebitCreditNotePdfReplyEvent reply =
                DebitCreditNotePdfReplyEvent.success(sagaId, sagaStep, correlationId, pdfUrl, pdfSize);
        outboxService.saveWithRouting(reply, AGGREGATE_TYPE, sagaId, REPLY_TOPIC, sagaId,
                toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "SUCCESS")));
        log.info("Published SUCCESS saga reply for saga {} step {}", sagaId, sagaStep);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        DebitCreditNotePdfReplyEvent reply =
                DebitCreditNotePdfReplyEvent.failure(sagaId, sagaStep, correlationId, errorMessage);
        outboxService.saveWithRouting(reply, AGGREGATE_TYPE, sagaId, REPLY_TOPIC, sagaId,
                toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "FAILURE")));
        log.info("Published FAILURE saga reply for saga {} step {}: {}", sagaId, sagaStep, errorMessage);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId) {
        DebitCreditNotePdfReplyEvent reply =
                DebitCreditNotePdfReplyEvent.compensated(sagaId, sagaStep, correlationId);
        outboxService.saveWithRouting(reply, AGGREGATE_TYPE, sagaId, REPLY_TOPIC, sagaId,
                toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "COMPENSATED")));
        log.info("Published COMPENSATED saga reply for saga {} step {}", sagaId, sagaStep);
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event headers", e);
        }
    }

    // Inline factory — replaces infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent
    private static class DebitCreditNotePdfReplyEvent extends SagaReply {
        private static final long serialVersionUID = 1L;
        private String pdfUrl;
        private Long pdfSize;

        private DebitCreditNotePdfReplyEvent(String sagaId, SagaStep sagaStep,
                                           String correlationId, ReplyStatus status,
                                           String pdfUrl, Long pdfSize) {
            super(sagaId, sagaStep, correlationId, status);
            this.pdfUrl  = pdfUrl;
            this.pdfSize = pdfSize;
        }

        private DebitCreditNotePdfReplyEvent(String sagaId, SagaStep sagaStep,
                                             String correlationId, String errorMessage) {
            super(sagaId, sagaStep, correlationId, errorMessage);
            this.pdfUrl  = null;
            this.pdfSize = null;
        }

        private DebitCreditNotePdfReplyEvent(String sagaId, SagaStep sagaStep,
                                             String correlationId, ReplyStatus status) {
            super(sagaId, sagaStep, correlationId, status);
            this.pdfUrl  = null;
            this.pdfSize = null;
        }

        public static DebitCreditNotePdfReplyEvent success(
                String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, Long pdfSize) {
            return new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.SUCCESS, pdfUrl, pdfSize);
        }

        public static DebitCreditNotePdfReplyEvent failure(
                String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
            return new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, errorMessage);
        }

        public static DebitCreditNotePdfReplyEvent compensated(
                String sagaId, SagaStep sagaStep, String correlationId) {
            return new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.COMPENSATED);
        }

        public String getPdfUrl()  { return pdfUrl; }
        public Long getPdfSize()   { return pdfSize; }
    }
}
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Should compile cleanly

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java
git commit -m "refactor: inline DebitCreditNotePdfReplyEvent factory in SagaReplyPublisher"
```

---

## Task 9: Delete old files from `domain/`, `infrastructure/adapter/in/kafka/`, and `application/usecase/`

**Files:**
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteProcessCommand.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteCompensateCommand.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java`
- Delete: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/ProcessDebitCreditNotePdfUseCase.java`
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/CompensateDebitCreditNotePdfUseCase.java`

- [ ] **Step 1: Delete all old files**

```bash
rm src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteProcessCommand.java
rm src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteCompensateCommand.java
rm src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java
rm src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java
rm src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent.java
rm src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/ProcessDebitCreditNotePdfUseCase.java
rm src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/CompensateDebitCreditNotePdfUseCase.java
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Clean compile

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: delete old saga command classes, KafkaCommandMapper, and usecase interfaces"
```

---

## Task 10: Delete `application/service/SagaCommandHandler` (original location)

**Files:**
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandler.java` (original location)
- Delete: `src/main/java/com/wpanther/debitcreditnote/pdf/application/event/DebitCreditNotePdfGeneratedEvent.java` (original location)

```bash
rm src/main/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandler.java
rm src/main/java/com/wpanther/debitcreditnote/pdf/application/event/DebitCreditNotePdfGeneratedEvent.java
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Clean compile

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: delete original SagaCommandHandler and moved event class"
```

---

## Task 11: Update `SagaRouteConfig` — use new DTO package, call use cases with plain params

**Files:**
- Modify: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java`

Key changes:
1. Import `DebitCreditNotePdfCommand` and `DebitCreditNotePdfCompensationCommand` from `dto/` package
2. Change `processUseCase.handle(cmd)` to `processUseCase.handle(cmd.getDocumentId(), cmd.getDocumentNumber(), cmd.getSignedXmlUrl(), cmd.getSagaId(), cmd.getSagaStep(), cmd.getCorrelationId())`
3. Same for compensation: `compensateUseCase.handle(cmd.getDocumentId(), cmd.getSagaId(), cmd.getSagaStep(), cmd.getCorrelationId())`
4. Remove `KafkaCommandMapper` usage
5. Update DLQ `onPrepareFailure` instanceof checks to use new DTO names

- [ ] **Step 1: Rewrite SagaRouteConfig**

Replace the entire `SagaRouteConfig.java` with this:

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.debitcreditnote.pdf.application.port.in.CompensateDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.application.port.in.ProcessDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCompensationCommand;
import com.wpanther.saga.domain.enums.SagaStep;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class SagaRouteConfig extends RouteBuilder {

    private final ProcessDebitCreditNotePdfUseCase processUseCase;
    private final CompensateDebitCreditNotePdfUseCase compensateUseCase;
    private final SagaCommandHandler sagaCommandHandler;
    private final ObjectMapper objectMapper;

    public SagaRouteConfig(ProcessDebitCreditNotePdfUseCase processUseCase,
                           CompensateDebitCreditNotePdfUseCase compensateUseCase,
                           SagaCommandHandler sagaCommandHandler,
                           ObjectMapper objectMapper) {
        this.processUseCase     = processUseCase;
        this.compensateUseCase  = compensateUseCase;
        this.sagaCommandHandler = sagaCommandHandler;
        this.objectMapper       = objectMapper;
    }

    @Override
    public void configure() throws Exception {

        errorHandler(deadLetterChannel(
                        "kafka:{{app.kafka.topics.dlq}}?brokers={{app.kafka.bootstrap-servers}}")
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2)
                        .maximumRedeliveryDelay(10000)
                        .logExhausted(true)
                        .logStackTrace(true)
                        .onPrepareFailure(exchange -> {
                            Throwable cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
                            Object body = exchange.getIn().getBody();
                            if (body instanceof DebitCreditNotePdfCommand cmd) {
                                log.error("DLQ: notifying orchestrator of retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentNumber());
                                sagaCommandHandler.publishOrchestrationFailure(cmd, cause);
                            } else if (body instanceof DebitCreditNotePdfCompensationCommand cmd) {
                                log.error("DLQ: notifying orchestrator of compensation retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentId());
                                sagaCommandHandler.publishCompensationOrchestrationFailure(cmd, cause);
                            } else {
                                log.error("DLQ: body not deserialized ({}); attempting saga metadata recovery",
                                        body == null ? "null" : body.getClass().getSimpleName());
                                recoverAndNotifyOrchestrator(body, cause);
                            }
                        }));

        from("kafka:{{app.kafka.topics.saga-command-debit-credit-note-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.command-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-command-consumer")
                .log(LoggingLevel.DEBUG, "Received saga command from Kafka: partition=${header[kafka.PARTITION]}, offset=${header[kafka.OFFSET]}")
                .unmarshal().json(JsonLibrary.Jackson, DebitCreditNotePdfCommand.class)
                .process(exchange -> {
                        DebitCreditNotePdfCommand cmd = exchange.getIn().getBody(DebitCreditNotePdfCommand.class);
                        log.info("Processing saga command for saga: {}, document: {}",
                                        cmd.getSagaId(), cmd.getDocumentNumber());
                        processUseCase.handle(
                                cmd.getDocumentId(),
                                cmd.getDocumentNumber(),
                                cmd.getSignedXmlUrl(),
                                cmd.getSagaId(),
                                cmd.getSagaStep(),
                                cmd.getCorrelationId());
                })
                .log("Successfully processed saga command");

        from("kafka:{{app.kafka.topics.saga-compensation-debit-credit-note-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.compensation-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-compensation-consumer")
                .log(LoggingLevel.DEBUG, "Received compensation command from Kafka: partition=${header[kafka.PARTITION]}, offset=${header[kafka.OFFSET]}")
                .unmarshal().json(JsonLibrary.Jackson, DebitCreditNotePdfCompensationCommand.class)
                .process(exchange -> {
                        DebitCreditNotePdfCompensationCommand cmd = exchange.getIn().getBody(DebitCreditNotePdfCompensationCommand.class);
                        log.info("Processing compensation for saga: {}, document: {}",
                                        cmd.getSagaId(), cmd.getDocumentId());
                        compensateUseCase.handle(
                                cmd.getDocumentId(),
                                cmd.getSagaId(),
                                cmd.getSagaStep(),
                                cmd.getCorrelationId());
                })
                .log("Successfully processed compensation command");
    }

    private void recoverAndNotifyOrchestrator(Object body, Throwable cause) {
        if (body == null) {
            log.error("DLQ: null message body — orchestrator must timeout");
            return;
        }
        try {
            byte[] rawBytes = body instanceof byte[] b
                    ? b
                    : body.toString().getBytes(StandardCharsets.UTF_8);
            JsonNode node        = objectMapper.readTree(rawBytes);
            String sagaId        = node.path("sagaId").asText(null);
            String sagaStepStr   = node.path("sagaStep").asText(null);
            String correlationId = node.path("correlationId").asText(null);

            if (sagaId == null || sagaStepStr == null) {
                log.error("DLQ: saga metadata missing in raw message — orchestrator must timeout");
                return;
            }
            SagaStep sagaStep = objectMapper.readValue("\"" + sagaStepStr + "\"", SagaStep.class);
            sagaCommandHandler.publishOrchestrationFailureForUnparsedMessage(
                    sagaId, sagaStep, correlationId, cause);
        } catch (Exception parseEx) {
            log.error("DLQ: cannot parse raw message for saga metadata — orchestrator must timeout", parseEx);
        }
    }
}
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Clean compile

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java
git commit -m "refactor: update SagaRouteConfig to use new DTO package and call use cases with plain params"
```

---

## Task 12: Update tests for new structure

**Files:**
- Modify: `src/test/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandlerTest.java`
- Delete: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java` (already deleted in Task 9)

The `SagaCommandHandlerTest` needs to be updated to:
1. Remove imports for `KafkaDebitCreditNoteProcessCommand` and `KafkaDebitCreditNoteCompensateCommand` — use `DebitCreditNotePdfCommand` and `DebitCreditNotePdfCompensationCommand` from `dto/` package
2. Update all method call sites where `handler.handle(command)` is called — change to `handler.handle(docId, docNumber, signedXmlUrl, sagaId, step, correlationId)` or `handler.handle(docId, sagaId, step, correlationId)`
3. Update `publishOrchestrationFailure` / `publishCompensationOrchestrationFailure` calls — change from passing a command object to passing `sagaId, step, correlationId, cause`
4. Update `verify` calls for `completeGenerationAndPublish`, `failGenerationAndPublish`, etc. — use plain parameters instead of `any()` for command

- [ ] **Step 1: Rewrite SagaCommandHandlerTest**

Replace the entire test file with this refactored version (key changes: imports updated to use new DTO package, method calls use plain parameters, verify stubs match new signatures):

```java
package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.port.out.PdfStoragePort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.service.DebitCreditNotePdfGenerationService;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.SagaCommandHandler;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCompensationCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCommand;
import com.wpanther.saga.domain.enums.SagaStep;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SagaCommandHandlerTest {

    @Mock private DebitCreditNotePdfDocumentService pdfDocumentService;
    @Mock private DebitCreditNotePdfGenerationService pdfGenerationService;
    @Mock private PdfStoragePort pdfStoragePort;
    @Mock private SagaReplyPort sagaReplyPort;
    @Mock private SignedXmlFetchPort signedXmlFetchPort;

    private SagaCommandHandler handler;

    // Test constants
    private static final String SAGA_ID = "saga-1";
    private static final SagaStep SAGA_STEP = SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF;
    private static final String CORRELATION_ID = "corr-1";
    private static final String DOCUMENT_ID = "dcn-001";
    private static final String DOCUMENT_NUMBER = "DCN-2024-001";
    private static final String SIGNED_XML_URL = "http://storage/signed.xml";

    @BeforeEach
    void setUp() {
        handler = new SagaCommandHandler(
                pdfDocumentService, pdfGenerationService,
                pdfStoragePort, sagaReplyPort, signedXmlFetchPort, 3);
    }

    private DebitCreditNotePdfDocument generatingDoc() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER).build();
        doc.startGeneration();
        return doc;
    }

    @Test
    void handle_process_success() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(SIGNED_XML_URL)).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(DOCUMENT_NUMBER, "<xml/>")).thenReturn(new byte[100]);
        when(pdfStoragePort.store(DOCUMENT_NUMBER, new byte[100])).thenReturn("2024/01/15/test.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/test.pdf")).thenReturn("http://minio/test.pdf");

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(doc.getId()), eq("2024/01/15/test.pdf"), eq("http://minio/test.pdf"),
                eq(100L), eq(-1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
    }

    @Test
    void handle_process_idempotentSuccess() {
        DebitCreditNotePdfDocument completedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.COMPLETED).documentUrl("http://minio/existing.pdf")
                .fileSize(9999L).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID))
                .thenReturn(Optional.of(completedDoc));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishIdempotentSuccess(
                eq(completedDoc), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_process_maxRetriesExceeded() {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.FAILED).retryCount(3).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID))
                .thenReturn(Optional.of(failedDoc));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishRetryExhausted(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_process_generationFailure() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString()))
                .thenThrow(new RuntimeException("FOP failed"));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("FOP failed"), eq(-1),
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_compensate_success() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .documentPath("2024/01/15/test.pdf").status(GenerationStatus.COMPLETED).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(doc));

        handler.handle(DOCUMENT_ID, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).deleteById(doc.getId());
        verify(pdfStoragePort).delete("2024/01/15/test.pdf");
        verify(pdfDocumentService).publishCompensated(SAGA_ID, SAGA_STEP, CORRELATION_ID);
    }

    @Test
    void handle_compensate_idempotent() {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());

        handler.handle(DOCUMENT_ID, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService, never()).deleteById(any());
        verify(pdfDocumentService).publishCompensated(SAGA_ID, SAGA_STEP, CORRELATION_ID);
    }

    @Test
    void handle_process_nullSignedXmlUrl() {
        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, null,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("signedXmlUrl"));
    }

    @Test
    void handle_process_blankSignedXmlUrl() {
        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, "   ",
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("signedXmlUrl"));
    }

    @Test
    void handle_process_nullDocumentId() {
        handler.handle(null, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("documentId"));
    }

    @Test
    void handle_process_nullDocumentNumber() {
        handler.handle(DOCUMENT_ID, null, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("documentNumber"));
    }

    @Test
    void handle_compensate_compensateFailure_deletesFromStorage() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .documentPath("2024/01/15/test.pdf").status(GenerationStatus.COMPLETED).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(doc));
        doThrow(new RuntimeException("DB error")).when(pdfDocumentService).deleteById(doc.getId());

        handler.handle(DOCUMENT_ID, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishCompensationFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("Compensation failed"));
    }

    @Test
    void handle_process_retryScenario() throws Exception {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.FAILED).retryCount(1).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(failedDoc));

        DebitCreditNotePdfDocument newDoc = generatingDoc();
        when(pdfDocumentService.replaceAndBeginGeneration(eq(failedDoc.getId()), eq(1),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER))).thenReturn(newDoc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/retry.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/retry.pdf")).thenReturn("http://minio/retry.pdf");

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(newDoc.getId()), eq("2024/01/15/retry.pdf"), eq("http://minio/retry.pdf"),
                eq(50L), eq(1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
    }

    @Test
    void handle_process_circuitBreakerOpen() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        CircuitBreaker cb = CircuitBreaker.of("test", CircuitBreakerConfig.ofDefaults());
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(CallNotPermittedException.createCallNotPermittedException(cb));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("Circuit breaker open"), eq(-1),
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_process_httpClientError() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(new HttpClientErrorException(
                        org.springframework.http.HttpStatus.NOT_FOUND, "Not Found"));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("HTTP error fetching signed XML"), eq(-1),
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_process_httpServerError() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(new HttpServerErrorException(
                        org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("HTTP error fetching signed XML"), eq(-1),
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_process_pdfGenerationFailure_cleansUpOrphanPdf() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/orphan.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/orphan.pdf"))
                .thenThrow(new RuntimeException("URL resolution failed"));
        doNothing().when(pdfStoragePort).delete(anyString());

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfStoragePort).delete("2024/01/15/orphan.pdf");
        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("URL resolution failed"), eq(-1),
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void publishOrchestrationFailure_notifiesOrchestrator() {
        DebitCreditNotePdfCommand cmd = new DebitCreditNotePdfCommand(
                SAGA_ID, SAGA_STEP, CORRELATION_ID,
                DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL);
        handler.publishOrchestrationFailure(cmd, new RuntimeException("DLQ reason"));

        verify(sagaReplyPort).publishFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                contains("DLQ reason"));
    }

    @Test
    void publishCompensationOrchestrationFailure_notifiesOrchestrator() {
        DebitCreditNotePdfCompensationCommand cmd = new DebitCreditNotePdfCompensationCommand(
                SAGA_ID, SAGA_STEP, CORRELATION_ID, DOCUMENT_ID);
        handler.publishCompensationOrchestrationFailure(cmd, new RuntimeException("Comp DLQ"));

        verify(sagaReplyPort).publishFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                contains("Compensation DLQ"));
    }

    @Test
    void publishOrchestrationFailureForUnparsedMessage_notifiesOrchestrator() {
        handler.publishOrchestrationFailureForUnparsedMessage(
                SAGA_ID, SAGA_STEP, CORRELATION_ID,
                new RuntimeException("deserialization failed"));

        verify(sagaReplyPort).publishFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                contains("deserialization failure"));
    }

    @Test
    void handle_process_withNoRetryNotMaxRetriesExceeded() throws Exception {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.FAILED).retryCount(1).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(failedDoc));

        DebitCreditNotePdfDocument newDoc = generatingDoc();
        when(pdfDocumentService.replaceAndBeginGeneration(eq(failedDoc.getId()), eq(1),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER))).thenReturn(newDoc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/retry.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/retry.pdf")).thenReturn("http://minio/retry.pdf");

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL,
                SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService, never()).publishRetryExhausted(any(), any(), any(), any(), any());
    }
}
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Clean compile

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandlerTest.java
git commit -m "refactor: update SagaCommandHandlerTest to use new DTO package and plain parameter signatures"
```

---

## Task 13: Build and test

**Files:**
- All modified files

- [ ] **Step 1: Full clean compile**

Run: `mvn clean compile 2>&1 | tail -20`
Expected: BUILD SUCCESS

- [ ] **Step 2: Run tests**

Run: `mvn clean test 2>&1 | tail -30`
Expected: BUILD SUCCESS (all tests pass)

- [ ] **Step 3: Commit all remaining changes**

```bash
git add -A
git commit -m "refactor: complete layer separation — saga types in infrastructure, plain-parameter use cases in application/port/in/"
```

---

## Verification Checklist

After all tasks:

```bash
# 1. Compile check
mvn clean compile -q

# 2. All tests pass
mvn clean test -q

# 3. Confirm new structure
ls src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/dto/
# Expected: DebitCreditNotePdfCommand.java, DebitCreditNotePdfCompensationCommand.java

ls src/main/java/com/wpanther/debitcreditnote/pdf/application/port/in/
# Expected: CompensateDebitCreditNotePdfUseCase.java, ProcessDebitCreditNotePdfUseCase.java

ls src/main/java/com/wpanther/debitcreditnote/pdf/application/dto/event/
# Expected: DebitCreditNotePdfGeneratedEvent.java

# 4. Confirm KafkaCommandMapper is gone
ls src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/
# Expected: SagaCommandHandler.java, SagaRouteConfig.java (NO KafkaCommandMapper)
```

---

## Self-Review Checklist

- [ ] `ProcessDebitCreditNotePdfUseCase.handle()` has document fields first, saga metadata last (matches reference)
- [ ] `CompensateDebitCreditNotePdfUseCase.handle()` has documentId first, saga metadata after (matches reference)
- [ ] `SagaCommandHandler` is in `infrastructure/adapter/in/kafka/`
- [ ] `DebitCreditNotePdfGeneratedEvent` is in `application/dto/event/`
- [ ] `DebitCreditNotePdfReplyEvent` is inlined in `SagaReplyPublisher`
- [ ] `KafkaCommandMapper` and its test are deleted
- [ ] All tests pass with `mvn clean test`
- [ ] Compensation flow publishes `COMPENSATION_FAILURE` (not `COMPENSATED`) when deletion fails
