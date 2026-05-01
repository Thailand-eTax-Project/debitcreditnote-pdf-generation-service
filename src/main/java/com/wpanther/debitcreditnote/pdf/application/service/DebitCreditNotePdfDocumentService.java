package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.dto.event.DebitCreditNotePdfGeneratedEvent;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.metrics.PdfGenerationMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DebitCreditNotePdfDocumentService {

    private final DebitCreditNotePdfDocumentRepository repository;
    private final PdfEventPort pdfEventPort;
    private final SagaReplyPort sagaReplyPort;
    private final PdfGenerationMetrics pdfGenerationMetrics;

    @Transactional(readOnly = true)
    public Optional<DebitCreditNotePdfDocument> findByDebitCreditNoteId(String debitCreditNoteId) {
        return repository.findByDebitCreditNoteId(debitCreditNoteId);
    }

    @Transactional
    public DebitCreditNotePdfDocument beginGeneration(String debitCreditNoteId, String documentNumber) {
        log.info("Initiating PDF generation for debit/credit note: {}", documentNumber);
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(debitCreditNoteId)
                .documentNumber(documentNumber)
                .build();
        doc.startGeneration();
        return repository.save(doc);
    }

    @Transactional
    public DebitCreditNotePdfDocument replaceAndBeginGeneration(
            UUID existingId, int previousRetryCount, String debitCreditNoteId, String documentNumber) {
        log.info("Replacing document {} and re-starting generation for: {}", existingId, documentNumber);
        repository.deleteById(existingId);
        repository.flush();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(debitCreditNoteId)
                .documentNumber(documentNumber)
                .build();
        doc.startGeneration();
        doc.incrementRetryCountTo(previousRetryCount + 1);
        return repository.save(doc);
    }

    @Transactional
    public void completeGenerationAndPublish(UUID documentId, String s3Key, String fileUrl,
                                             long fileSize, int previousRetryCount,
                                             String sagaId, SagaStep sagaStep, String correlationId,
                                             String documentIdField, String documentNumber) {
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markCompleted(s3Key, fileUrl, fileSize);
        doc.markXmlEmbedded();
        applyRetryCount(doc, previousRetryCount);
        doc = repository.save(doc);

        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(doc, sagaId, documentIdField, documentNumber, correlationId));
        sagaReplyPort.publishSuccess(sagaId, sagaStep, correlationId, doc.getDocumentUrl(), doc.getFileSize());

        log.info("Completed PDF generation for saga {} debit/credit note {}", sagaId, doc.getDocumentNumber());
    }

    @Transactional
    public void failGenerationAndPublish(UUID documentId, String errorMessage,
                                         int previousRetryCount,
                                         String sagaId, SagaStep sagaStep, String correlationId) {
        String safeError = errorMessage != null ? errorMessage : "PDF generation failed";
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markFailed(safeError);
        applyRetryCount(doc, previousRetryCount);
        repository.save(doc);
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, safeError);
        log.warn("PDF generation failed for saga {} debit/credit note {}: {}", sagaId, doc.getDocumentNumber(), safeError);
    }

    @Transactional
    public void deleteById(UUID documentId) {
        repository.deleteById(documentId);
        repository.flush();
    }

    @Transactional
    public void publishIdempotentSuccess(DebitCreditNotePdfDocument existing,
                                         String sagaId, SagaStep sagaStep, String correlationId,
                                         String documentIdField, String documentNumber) {
        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(existing, sagaId, documentIdField, documentNumber, correlationId));
        sagaReplyPort.publishSuccess(sagaId, sagaStep, correlationId, existing.getDocumentUrl(), existing.getFileSize());
        log.warn("Debit/credit note PDF already generated for saga {} — re-publishing SUCCESS reply", sagaId);
    }

    @Transactional
    public void publishRetryExhausted(String sagaId, SagaStep sagaStep, String correlationId,
                                       String documentIdField, String documentNumber) {
        pdfGenerationMetrics.recordRetryExhausted(sagaId, documentIdField, documentNumber);
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, "Maximum retry attempts exceeded");
        log.error("Max retries exceeded for saga {} document {}", sagaId, documentNumber);
    }

    @Transactional
    public void publishGenerationFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, errorMessage);
    }

    @Transactional
    public void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId) {
        sagaReplyPort.publishCompensated(sagaId, sagaStep, correlationId);
    }

    @Transactional
    public void publishCompensationFailure(String sagaId, SagaStep sagaStep, String correlationId, String error) {
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, error);
    }

    private DebitCreditNotePdfDocument requireDocument(UUID documentId) {
        return repository.findById(documentId)
                .orElseThrow(() -> new IllegalStateException(
                        "Expected debit/credit note PDF document is absent: " + documentId));
    }

    private void applyRetryCount(DebitCreditNotePdfDocument doc, int previousRetryCount) {
        if (previousRetryCount < 0) return;
        doc.incrementRetryCountTo(previousRetryCount + 1);
    }

    private DebitCreditNotePdfGeneratedEvent buildGeneratedEvent(DebitCreditNotePdfDocument doc,
                                                                  String sagaId, String documentIdField,
                                                                  String documentNumber, String correlationId) {
        return new DebitCreditNotePdfGeneratedEvent(
                sagaId, documentIdField, documentNumber,
                doc.getDocumentUrl(), doc.getFileSize(), doc.isXmlEmbedded(), correlationId);
    }
}
