package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;

/**
 * Use case for compensating debit/credit note PDF generation saga commands.
 * Full implementation in Task 11.
 */
public interface CompensateDebitCreditNotePdfUseCase {
    void handle(KafkaDebitCreditNoteCompensateCommand command);
}
