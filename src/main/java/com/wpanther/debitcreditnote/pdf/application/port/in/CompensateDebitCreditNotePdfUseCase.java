package com.wpanther.debitcreditnote.pdf.application.port.in;

import com.wpanther.saga.domain.enums.SagaStep;

/**
 * Inbound port for debit/credit note PDF compensation.
 * Called by SagaCommandHandler with plain fields — no command objects.
 */
public interface CompensateDebitCreditNotePdfUseCase {

    void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId);
}