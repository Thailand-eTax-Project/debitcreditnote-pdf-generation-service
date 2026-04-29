package com.wpanther.debitcreditnote.pdf.application.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DebitCreditNotePdfGeneratedEventTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void constructor_setsAllFields() {
        DebitCreditNotePdfGeneratedEvent event = new DebitCreditNotePdfGeneratedEvent(
                "saga-1", "dcn-001", "DCN-2024-001",
                "http://minio/dc.pdf", 5000L, true, "corr-1");

        assertThat(event.getSagaId()).isEqualTo("saga-1");
        assertThat(event.getCorrelationId()).isEqualTo("corr-1");
        assertThat(event.getDocumentId()).isEqualTo("dcn-001");
        assertThat(event.getDocumentNumber()).isEqualTo("DCN-2024-001");
        assertThat(event.getDocumentUrl()).isEqualTo("http://minio/dc.pdf");
        assertThat(event.getFileSize()).isEqualTo(5000L);
        assertThat(event.isXmlEmbedded()).isTrue();
        assertThat(event.getEventType()).isEqualTo("pdf.generated.debit-credit-note");
        assertThat(event.getSource()).isEqualTo("debitcreditnote-pdf-generation-service");
        assertThat(event.getTraceType()).isEqualTo("PDF_GENERATED");
    }

    @Test
    void jsonCreator_constructor_setsAllFields() {
        DebitCreditNotePdfGeneratedEvent event = new DebitCreditNotePdfGeneratedEvent(
                UUID.randomUUID(), Instant.now(), "pdf.generated.debit-credit-note", 1,
                "saga-1", "corr-1", "debitcreditnote-pdf-generation-service",
                "PDF_GENERATED", null,
                "dcn-001", "DCN-2024-001", "http://minio/dc.pdf", 5000L, true);

        assertThat(event.getDocumentId()).isEqualTo("dcn-001");
        assertThat(event.getDocumentNumber()).isEqualTo("DCN-2024-001");
        assertThat(event.getDocumentUrl()).isEqualTo("http://minio/dc.pdf");
        assertThat(event.getFileSize()).isEqualTo(5000L);
        assertThat(event.isXmlEmbedded()).isTrue();
    }

    @Test
    void serialization_roundTrip() throws Exception {
        DebitCreditNotePdfGeneratedEvent original = new DebitCreditNotePdfGeneratedEvent(
                "saga-1", "dcn-001", "DCN-2024-001",
                "http://minio/dc.pdf", 5000L, true, "corr-1");

        String json = objectMapper.writeValueAsString(original);
        DebitCreditNotePdfGeneratedEvent deserialized = objectMapper.readValue(json, DebitCreditNotePdfGeneratedEvent.class);

        assertThat(deserialized.getSagaId()).isEqualTo("saga-1");
        assertThat(deserialized.getDocumentId()).isEqualTo("dcn-001");
        assertThat(deserialized.getDocumentNumber()).isEqualTo("DCN-2024-001");
        assertThat(deserialized.getDocumentUrl()).isEqualTo("http://minio/dc.pdf");
        assertThat(deserialized.getFileSize()).isEqualTo(5000L);
        assertThat(deserialized.isXmlEmbedded()).isTrue();
        assertThat(deserialized.getCorrelationId()).isEqualTo("corr-1");
    }
}
