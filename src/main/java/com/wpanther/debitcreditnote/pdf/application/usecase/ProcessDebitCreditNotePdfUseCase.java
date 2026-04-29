package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;

public interface ProcessDebitCreditNotePdfUseCase {
    void handle(KafkaDebitCreditNoteProcessCommand command);
}
