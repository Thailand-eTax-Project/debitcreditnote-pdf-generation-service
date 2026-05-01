# debitcreditnote-pdf-generation-service Layer Separation Refactoring

**Date:** 2026-05-01
**Author:** Claude Code
**Status:** Approved

## Context

`debitcreditnote-pdf-generation-service` has saga infrastructure types (`SagaCommand` subclasses + Jackson annotations, `SagaReply` subclasses) and use case interfaces with command objects placed in `application/usecase/`. This violates Hexagonal/Port-Adapters architecture — the domain and application layers should have no framework or infrastructure dependencies.

The reference implementation is `invoice-pdf-generation-service` (and by extension `taxinvoice-processing-service`), which correctly separates:
- Kafka DTOs with saga inheritance and Jackson annotations → `infrastructure/adapter/in/kafka/dto/`
- Use case interfaces with plain parameters → `application/port/in/`
- Saga command handler (driving adapter) → `infrastructure/adapter/in/kafka/`
- Notification events extending `TraceEvent` → `application/dto/event/`
- Reply event factories inlined in `SagaReplyPublisher`

## Problem Statement

The service has three architecture violations:

1. **`KafkaDebitCreditNoteProcessCommand` and `KafkaDebitCreditNoteCompensateCommand`** are in `infrastructure/adapter/in/kafka/` but also implement `ProcessDebitCreditNotePdfUseCase.Command` / `CompensateDebitCreditNotePdfUseCase.Command` — a framework interface from `application/usecase/`. This couples the infrastructure DTO to the application layer.

2. **Use case interfaces** in `application/usecase/` accept command objects (`ProcessDebitCreditNotePdfUseCase.Command`) instead of plain fields. Command objects carry `extends SagaCommand` types into the application layer, leaking infrastructure concerns.

3. **`DebitCreditNotePdfReplyEvent`** in `infrastructure/adapter/out/messaging/` extends `SagaReply` but is not inlined — it's a separate class only used as a factory by `SagaReplyPublisher`.

4. **`DebitCreditNotePdfGeneratedEvent`** extends `TraceEvent` (saga library) and has Jackson annotations — it belongs in `application/dto/event/` with other notification DTOs, not in `application/event/`.

## Design

### Target Architecture

```
infrastructure/adapter/in/kafka/dto/
├── DebitCreditNotePdfCommand              ← (rename from KafkaDebitCreditNoteProcessCommand)
│                                            extends SagaCommand + Jackson (Kafka deserialization)
└── DebitCreditNotePdfCompensationCommand  ← (rename from KafkaDebitCreditNoteCompensateCommand)
                                             extends SagaCommand + Jackson (Kafka deserialization)

infrastructure/adapter/in/kafka/
├── SagaCommandHandler                     ← (moved from application/service/)
├── SagaRouteConfig                         ← (updated to use new DTO package)
└── KafkaCommandMapper                      ← (DELETE — identity mapper, no longer needed)

application/port/in/
├── ProcessDebitCreditNotePdfUseCase        ← (refactored from usecase/, plain params)
└── CompensateDebitCreditNotePdfUseCase     ← (refactored from usecase/, plain params)

application/dto/event/
└── DebitCreditNotePdfGeneratedEvent       ← (moved from application/event/, extends TraceEvent)

application/service/
└── DebitCreditNotePdfDocumentService      ← (modified — plain parameters, no command objects)

infrastructure/adapter/out/messaging/
├── SagaReplyPublisher                     ← (unchanged interface, inlined reply factory)
└── OutboxConstants                         ← (unchanged)

application/event/                          ← (DELETE — empty after move)
application/usecase/                       ← (DELETE — interfaces moved to port/in/)
```

### Changes

#### 1. `infrastructure/adapter/in/kafka/dto/`

**Create `DebitCreditNotePdfCommand`** — rename of `KafkaDebitCreditNoteProcessCommand`:
- Package: `com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto`
- Removes `implements ProcessDebitCreditNotePdfUseCase.Command`
- `extends SagaCommand` + Jackson annotations stay (Kafka deserialization concern)
- Two constructors: `@JsonCreator` Jackson constructor + convenience constructor for testing

**Create `DebitCreditNotePdfCompensationCommand`** — rename of `KafkaDebitCreditNoteCompensateCommand`:
- Package: `com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto`
- Removes `implements CompensateDebitCreditNotePdfUseCase.Command`
- Same pattern as above

#### 2. `application/port/in/`

**`ProcessDebitCreditNotePdfUseCase`** — refactored from `application/usecase/`:
```java
public interface ProcessDebitCreditNotePdfUseCase {
    void handle(String documentId, String documentNumber, String signedXmlUrl,
                String sagaId, SagaStep sagaStep, String correlationId);
}
```

**`CompensateDebitCreditNotePdfUseCase`** — refactored from `application/usecase/`:
```java
public interface CompensateDebitCreditNotePdfUseCase {
    void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId);
}
```

#### 3. `infrastructure/adapter/in/kafka/SagaCommandHandler`

**Move from** `application/service/SagaCommandHandler.java`
**To** `infrastructure/adapter/in/kafka/SagaCommandHandler.java`

Now implements the new plain-parameter use case interfaces:
- `SagaCommandHandler implements ProcessDebitCreditNotePdfUseCase, CompensateDebitCreditNotePdfUseCase`
- Extracts all fields from `DebitCreditNotePdfCommand` / `DebitCreditNotePdfCompensationCommand` and calls use case methods with plain parameters
- Calls `DebitCreditNotePdfDocumentService` with plain parameters (not command objects)
- No command objects flow into the application or domain layers

**Compensation flow fix:** When document deletion fails, do NOT publish `COMPENSATED` — publish `COMPENSATION_FAILURE` instead. The current code incorrectly publishes COMPENSATED even when deletion fails; the new handler only publishes COMPENSATED on success.

#### 4. `application/service/DebitCreditNotePdfDocumentService`

**Modified method signatures.** Methods that currently accept `ProcessDebitCreditNotePdfUseCase.Command` are updated to accept individual fields:

| Method | New Signature |
|--------|---------------|
| `completeGenerationAndPublish` | `(UUID id, String s3Key, String fileUrl, long fileSize, int retryCount, String sagaId, SagaStep sagaStep, String correlationId, String documentId, String documentNumber)` |
| `failGenerationAndPublish` | `(UUID id, String error, int retryCount, String sagaId, SagaStep sagaStep, String correlationId)` |
| `publishIdempotentSuccess` | `(DebitCreditNotePdfDocument existing, String sagaId, SagaStep sagaStep, String correlationId, String documentId, String documentNumber)` |
| `publishRetryExhausted` | `(String sagaId, SagaStep sagaStep, String correlationId, String documentId, String documentNumber)` |
| `publishGenerationFailure` | `(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage)` |
| `publishCompensated` | `(String sagaId, SagaStep sagaStep, String correlationId)` |
| `publishCompensationFailure` | `(String sagaId, SagaStep sagaStep, String correlationId, String error)` |
| `buildGeneratedEvent` | `(DebitCreditNotePdfDocument doc, String sagaId, String correlationId, String documentId, String documentNumber)` |

#### 5. `application/dto/event/DebitCreditNotePdfGeneratedEvent`

**Move from** `application/event/DebitCreditNotePdfGeneratedEvent`
**To** `application/dto/event/DebitCreditNotePdfGeneratedEvent`

Package: `com.wpanther.debitcreditnote.pdf.application.dto.event`

`DebitCreditNotePdfGeneratedEvent` extends `TraceEvent` (saga library) — it's a notification DTO, not a domain event. Its proper home is `application/dto/event/`.

#### 6. `infrastructure/adapter/out/messaging/SagaReplyPublisher`

**Unchanged interface**, but inline the `DebitCreditNotePdfReplyEvent` factory as private static inner class (or static factory methods). The class `DebitCreditNotePdfReplyEvent` in `infrastructure/adapter/out/messaging/` is deleted after inlining.

#### 7. `infrastructure/adapter/in/kafka/KafkaCommandMapper` — DELETE

The mapper is an identity mapper (returns its input unchanged). It becomes unnecessary after `SagaCommandHandler` is rewritten to extract fields directly from DTOs.

#### 8. `application/event/` — DELETE

After moving `DebitCreditNotePdfGeneratedEvent`, this directory is empty.

#### 9. `application/usecase/` — DELETE

After moving interfaces to `application/port/in/`, this directory is empty.

#### 10. `SagaRouteConfig`

**Updated to:**
- Import `DebitCreditNotePdfCommand` and `DebitCreditNotePdfCompensationCommand` from `dto/` package
- Route processors call `processUseCase.handle(docId, docNum, signedXmlUrl, sagaId, step, correlationId)` with plain extracted fields (no command objects passed to use cases)
- Remove `KafkaCommandMapper` usage
- DLQ `onPrepareFailure` block uses the new DTO types from `dto/` package

## Files to Modify

| Action | File |
|--------|------|
| Create | `infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCommand.java` |
| Create | `infrastructure/adapter/in/kafka/dto/DebitCreditNotePdfCompensationCommand.java` |
| Create | `application/port/in/ProcessDebitCreditNotePdfUseCase.java` |
| Create | `application/port/in/CompensateDebitCreditNotePdfUseCase.java` |
| Move | `application/service/SagaCommandHandler.java` → `infrastructure/adapter/in/kafka/SagaCommandHandler.java` |
| Move | `application/event/DebitCreditNotePdfGeneratedEvent.java` → `application/dto/event/DebitCreditNotePdfGeneratedEvent.java` |
| Delete | `infrastructure/adapter/in/kafka/KafkaCommandMapper.java` |
| Delete | `infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java` |
| Delete | `infrastructure/adapter/in/kafka/KafkaDebitCreditNoteProcessCommand.java` |
| Delete | `infrastructure/adapter/in/kafka/KafkaDebitCreditNoteCompensateCommand.java` |
| Delete | `infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent.java` |
| Delete | `application/event/DebitCreditNotePdfGeneratedEvent.java` (original location) |
| Delete | `application/service/SagaCommandHandler.java` (original location) |
| Delete | `application/usecase/ProcessDebitCreditNotePdfUseCase.java` (original) |
| Delete | `application/usecase/CompensateDebitCreditNotePdfUseCase.java` (original) |
| Modify | `SagaRouteConfig.java` (use new DTO package, remove KafkaCommandMapper) |
| Modify | `DebitCreditNotePdfDocumentService.java` (plain parameter signatures) |
| Modify | `SagaReplyPublisher.java` (inline DebitCreditNotePdfReplyEvent factory) |
| Modify | `EventPublisher.java` (import new package path for DebitCreditNotePdfGeneratedEvent) |

## Verification

After refactoring, run:
```bash
mvn clean compile   # Verify no compilation errors
mvn clean test      # Verify all tests pass
```

## Scope

This refactor addresses **only** `debitcreditnote-pdf-generation-service`. Other services with similar patterns should be audited separately.