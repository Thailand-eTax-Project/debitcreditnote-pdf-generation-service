package com.wpanther.debitcreditnote.pdf.application.event;

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
