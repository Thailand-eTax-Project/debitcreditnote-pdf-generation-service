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