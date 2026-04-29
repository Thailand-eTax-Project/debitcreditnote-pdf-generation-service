package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;

/**
 * Use case for processing debit/credit note PDF generation saga commands.
 * Full implementation in Task 11.
 */
public interface ProcessDebitCreditNotePdfUseCase {
    void handle(KafkaDebitCreditNoteProcessCommand command);
}
