package com.wpanther.debitcreditnote.pdf.domain.repository;

import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository interface for DebitCreditNotePdfDocument aggregate
 */
public interface DebitCreditNotePdfDocumentRepository {

    /**
     * Save debit/credit note PDF document
     */
    DebitCreditNotePdfDocument save(DebitCreditNotePdfDocument document);

    /**
     * Find by ID
     */
    Optional<DebitCreditNotePdfDocument> findById(UUID id);

    /**
     * Find by debit/credit note ID
     */
    Optional<DebitCreditNotePdfDocument> findByDebitCreditNoteId(String debitCreditNoteId);

    /**
     * Delete by ID
     */
    void deleteById(UUID id);

    /**
     * Flush pending changes to the database.
     * Required to order a DELETE before a subsequent INSERT on the same unique key
     * within the same transaction (e.g., delete-then-recreate on saga retry).
     */
    void flush();
}
