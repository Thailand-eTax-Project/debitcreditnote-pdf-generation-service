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