package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
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
public class EventPublisher implements PdfEventPort {

    private static final String AGGREGATE_TYPE = OutboxConstants.AGGREGATE_TYPE;

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishPdfGenerated(DebitCreditNotePdfGeneratedEvent event) {
        Map<String, String> headers = Map.of(
            "documentType", "DEBIT_CREDIT_NOTE",
            "correlationId", event.getCorrelationId()
        );
        outboxService.saveWithRouting(
            event, AGGREGATE_TYPE, event.getDocumentId(),
            "pdf.generated.debit-credit-note", event.getDocumentId(), toJson(headers));
        log.info("Published DebitCreditNotePdfGeneratedEvent to outbox: {}", event.getDocumentNumber());
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event headers", e);
        }
    }
}
