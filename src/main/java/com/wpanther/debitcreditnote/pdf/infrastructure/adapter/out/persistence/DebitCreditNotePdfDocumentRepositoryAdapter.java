package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class DebitCreditNotePdfDocumentRepositoryAdapter implements DebitCreditNotePdfDocumentRepository {

    private final JpaDebitCreditNotePdfDocumentRepository jpaRepository;

    @Override
    public DebitCreditNotePdfDocument save(DebitCreditNotePdfDocument document) {
        return toDomain(jpaRepository.save(toEntity(document)));
    }

    @Override
    public Optional<DebitCreditNotePdfDocument> findById(UUID id) {
        return jpaRepository.findById(id).map(this::toDomain);
    }

    @Override
    public Optional<DebitCreditNotePdfDocument> findByDebitCreditNoteId(String debitCreditNoteId) {
        return jpaRepository.findByDebitCreditNoteId(debitCreditNoteId).map(this::toDomain);
    }

    @Override
    public void deleteById(UUID id) { jpaRepository.deleteById(id); }

    @Override
    public void flush() { jpaRepository.flush(); }

    private DebitCreditNotePdfDocumentEntity toEntity(DebitCreditNotePdfDocument d) {
        return DebitCreditNotePdfDocumentEntity.builder()
            .id(d.getId()).debitCreditNoteId(d.getDebitCreditNoteId())
            .documentNumber(d.getDocumentNumber()).documentPath(d.getDocumentPath())
            .documentUrl(d.getDocumentUrl()).fileSize(d.getFileSize())
            .mimeType(d.getMimeType()).xmlEmbedded(d.isXmlEmbedded())
            .status(d.getStatus()).errorMessage(d.getErrorMessage())
            .retryCount(d.getRetryCount()).createdAt(d.getCreatedAt())
            .completedAt(d.getCompletedAt()).build();
    }

    private DebitCreditNotePdfDocument toDomain(DebitCreditNotePdfDocumentEntity e) {
        return DebitCreditNotePdfDocument.builder()
            .id(e.getId()).debitCreditNoteId(e.getDebitCreditNoteId())
            .documentNumber(e.getDocumentNumber()).documentPath(e.getDocumentPath())
            .documentUrl(e.getDocumentUrl())
            .fileSize(e.getFileSize() != null ? e.getFileSize() : 0L)
            .mimeType(e.getMimeType())
            .xmlEmbedded(e.getXmlEmbedded() != null && e.getXmlEmbedded())
            .status(e.getStatus()).errorMessage(e.getErrorMessage())
            .retryCount(e.getRetryCount() != null ? e.getRetryCount() : 0)
            .createdAt(e.getCreatedAt()).completedAt(e.getCompletedAt()).build();
    }
}
