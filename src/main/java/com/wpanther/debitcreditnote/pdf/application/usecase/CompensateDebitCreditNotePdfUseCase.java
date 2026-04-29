package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;

public interface CompensateDebitCreditNotePdfUseCase {
    void handle(KafkaDebitCreditNoteCompensateCommand command);
}
