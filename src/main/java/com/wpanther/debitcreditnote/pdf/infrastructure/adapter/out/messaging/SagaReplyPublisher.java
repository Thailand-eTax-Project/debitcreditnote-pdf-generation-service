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

    private static class DebitCreditNotePdfReplyEvent extends SagaReply {

        private static final long serialVersionUID = 1L;

        private String pdfUrl;
        private Long pdfSize;

        public static DebitCreditNotePdfReplyEvent success(
                String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, Long pdfSize) {
            DebitCreditNotePdfReplyEvent reply =
                    new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.SUCCESS);
            reply.pdfUrl  = pdfUrl;
            reply.pdfSize = pdfSize;
            return reply;
        }

        public static DebitCreditNotePdfReplyEvent failure(
                String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
            return new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, errorMessage);
        }

        public static DebitCreditNotePdfReplyEvent compensated(
                String sagaId, SagaStep sagaStep, String correlationId) {
            return new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.COMPENSATED);
        }

        private DebitCreditNotePdfReplyEvent(String sagaId, SagaStep sagaStep,
                                             String correlationId, ReplyStatus status) {
            super(sagaId, sagaStep, correlationId, status);
        }

        private DebitCreditNotePdfReplyEvent(String sagaId, SagaStep sagaStep,
                                             String correlationId, String errorMessage) {
            super(sagaId, sagaStep, correlationId, errorMessage);
        }

        public String getPdfUrl()  { return pdfUrl; }
        public Long getPdfSize()   { return pdfSize; }
    }
}
