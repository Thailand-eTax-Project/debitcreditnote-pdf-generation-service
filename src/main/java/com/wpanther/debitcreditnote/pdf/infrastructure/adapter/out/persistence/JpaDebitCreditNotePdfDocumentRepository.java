package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public interface JpaDebitCreditNotePdfDocumentRepository
        extends JpaRepository<DebitCreditNotePdfDocumentEntity, UUID> {

    Optional<DebitCreditNotePdfDocumentEntity> findByDebitCreditNoteId(String debitCreditNoteId);

    @Query("SELECT e.documentPath FROM DebitCreditNotePdfDocumentEntity e WHERE e.documentPath IS NOT NULL")
    Set<String> findAllDocumentPaths();
}
