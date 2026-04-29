package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.saga.domain.enums.SagaStep;

public interface ProcessDebitCreditNotePdfUseCase {

    interface Command {
        String getSagaId();
        SagaStep getSagaStep();
        String getCorrelationId();
        String getDocumentId();
        String getDocumentNumber();
        String getSignedXmlUrl();
    }

    void handle(Command command);
}
