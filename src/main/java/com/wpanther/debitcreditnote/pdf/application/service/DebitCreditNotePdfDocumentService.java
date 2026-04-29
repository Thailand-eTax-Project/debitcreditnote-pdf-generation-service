package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging.DebitCreditNotePdfGeneratedEvent;
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
                                             KafkaDebitCreditNoteProcessCommand command) {
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markCompleted(s3Key, fileUrl, fileSize);
        doc.markXmlEmbedded();
        applyRetryCount(doc, previousRetryCount);
        doc = repository.save(doc);

        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(doc, command));
        sagaReplyPort.publishSuccess(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                doc.getDocumentUrl(), doc.getFileSize());

        log.info("Completed PDF generation for saga {} debit/credit note {}",
                command.getSagaId(), doc.getDocumentNumber());
    }

    @Transactional
    public void failGenerationAndPublish(UUID documentId, String errorMessage,
                                         int previousRetryCount,
                                         KafkaDebitCreditNoteProcessCommand command) {
        String safeError = errorMessage != null ? errorMessage : "PDF generation failed";
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markFailed(safeError);
        applyRetryCount(doc, previousRetryCount);
        repository.save(doc);
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), safeError);
        log.warn("PDF generation failed for saga {} debit/credit note {}: {}",
                command.getSagaId(), doc.getDocumentNumber(), safeError);
    }

    @Transactional
    public void deleteById(UUID documentId) {
        repository.deleteById(documentId);
        repository.flush();
    }

    @Transactional
    public void publishIdempotentSuccess(DebitCreditNotePdfDocument existing,
                                         KafkaDebitCreditNoteProcessCommand command) {
        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(existing, command));
        sagaReplyPort.publishSuccess(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                existing.getDocumentUrl(), existing.getFileSize());
        log.warn("Debit/credit note PDF already generated for saga {} — re-publishing SUCCESS reply",
                command.getSagaId());
    }

    @Transactional
    public void publishRetryExhausted(KafkaDebitCreditNoteProcessCommand command) {
        pdfGenerationMetrics.recordRetryExhausted(
                command.getSagaId(), command.getDocumentId(), command.getDocumentNumber());
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                "Maximum retry attempts exceeded");
        log.error("Max retries exceeded for saga {} document {}", command.getSagaId(), command.getDocumentNumber());
    }

    @Transactional
    public void publishGenerationFailure(KafkaDebitCreditNoteProcessCommand command, String errorMessage) {
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), errorMessage);
    }

    @Transactional
    public void publishCompensated(KafkaDebitCreditNoteCompensateCommand command) {
        sagaReplyPort.publishCompensated(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId());
    }

    @Transactional
    public void publishCompensationFailure(KafkaDebitCreditNoteCompensateCommand command, String error) {
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), error);
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
                                                                  KafkaDebitCreditNoteProcessCommand command) {
        return new DebitCreditNotePdfGeneratedEvent(
                command.getSagaId(), command.getDocumentId(), doc.getDocumentNumber(),
                doc.getDocumentUrl(), doc.getFileSize(), doc.isXmlEmbedded(), command.getCorrelationId());
    }
}
