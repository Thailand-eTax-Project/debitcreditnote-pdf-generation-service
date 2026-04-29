package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.saga.domain.enums.SagaStep;

public interface CompensateDebitCreditNotePdfUseCase {

    interface Command {
        String getSagaId();
        SagaStep getSagaStep();
        String getCorrelationId();
        String getDocumentId();
    }

    void handle(Command command);
}
