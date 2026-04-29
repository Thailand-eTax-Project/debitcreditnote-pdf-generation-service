package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.debitcreditnote.pdf.application.usecase.ProcessDebitCreditNotePdfUseCase;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

public class KafkaDebitCreditNoteProcessCommand extends SagaCommand
        implements ProcessDebitCreditNotePdfUseCase.Command {

    private static final long serialVersionUID = 1L;

    @Getter @JsonProperty("documentId")     private final String documentId;
    @Getter @JsonProperty("documentNumber") private final String documentNumber;
    @Getter @JsonProperty("signedXmlUrl")   private final String signedXmlUrl;

    @JsonCreator
    public KafkaDebitCreditNoteProcessCommand(
            @JsonProperty("eventId")        UUID eventId,
            @JsonProperty("occurredAt")     Instant occurredAt,
            @JsonProperty("eventType")      String eventType,
            @JsonProperty("version")        int version,
            @JsonProperty("sagaId")         String sagaId,
            @JsonProperty("sagaStep")       SagaStep sagaStep,
            @JsonProperty("correlationId")  String correlationId,
            @JsonProperty("documentId")     String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("signedXmlUrl")   String signedXmlUrl) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl   = signedXmlUrl;
    }

    /** Convenience constructor for testing. */
    public KafkaDebitCreditNoteProcessCommand(String sagaId, SagaStep sagaStep, String correlationId,
                                              String documentId, String documentNumber, String signedXmlUrl) {
        super(sagaId, sagaStep, correlationId);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl   = signedXmlUrl;
    }

    @Override public String getSagaId()        { return super.getSagaId(); }
    @Override public SagaStep getSagaStep()    { return super.getSagaStep(); }
    @Override public String getCorrelationId() { return super.getCorrelationId(); }
}
