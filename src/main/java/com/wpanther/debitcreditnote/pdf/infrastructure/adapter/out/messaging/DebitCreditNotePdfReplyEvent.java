package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaReply;

public class DebitCreditNotePdfReplyEvent extends SagaReply {

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
