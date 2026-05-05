package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.port.out.DocumentArchivePort;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.metrics.PdfGenerationMetrics;
import com.wpanther.saga.domain.enums.SagaStep;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DebitCreditNotePdfDocumentServiceTest {

    @Mock private DebitCreditNotePdfDocumentRepository repository;
    @Mock private PdfEventPort pdfEventPort;
    @Mock private SagaReplyPort sagaReplyPort;
    @Mock private DocumentArchivePort documentArchivePort;
    @Mock private PdfGenerationMetrics metrics;
    @InjectMocks private DebitCreditNotePdfDocumentService service;

    private static final String SAGA_ID = "saga-1";
    private static final SagaStep SAGA_STEP = SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF;
    private static final String CORRELATION_ID = "corr-1";
    private static final String DOCUMENT_ID = "dcn-001";
    private static final String DOCUMENT_NUMBER = "DCN-2024-001";

    @Test
    void findByDebitCreditNoteId_delegatesToRepository() {
        when(repository.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        assertThat(service.findByDebitCreditNoteId("dcn-001")).isEmpty();
    }

    @Test
    void findByDebitCreditNoteId_returnsDocumentWhenPresent() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001").build();
        when(repository.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.of(doc));
        assertThat(service.findByDebitCreditNoteId("dcn-001")).isPresent();
    }

    @Test
    void beginGeneration_savesGeneratingDocument() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.save(any())).thenReturn(doc);

        DebitCreditNotePdfDocument result = service.beginGeneration("dcn-001", "DCN-2024-001");

        assertThat(result.getStatus()).isEqualTo(GenerationStatus.GENERATING);
    }

    @Test
    void replaceAndBeginGeneration_deletesExistingAndSavesNew() {
        UUID existingId = UUID.randomUUID();
        DebitCreditNotePdfDocument newDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.save(any())).thenReturn(newDoc);

        DebitCreditNotePdfDocument result = service.replaceAndBeginGeneration(
                existingId, 1, "dcn-001", "DCN-2024-001");

        verify(repository).deleteById(existingId);
        verify(repository).flush();
        assertThat(result.getStatus()).isEqualTo(GenerationStatus.GENERATING);
    }

    @Test
    void completeGenerationAndPublish_marksCompletedAndPublishes() {
        UUID docId = UUID.randomUUID();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(docId).debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.findById(docId)).thenReturn(Optional.of(doc));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.completeGenerationAndPublish(
                docId, "2024/01/15/test.pdf", "http://minio/test.pdf", 12345L, -1,
                SAGA_ID, SAGA_STEP, CORRELATION_ID, DOCUMENT_ID, DOCUMENT_NUMBER);

        verify(documentArchivePort).publish(any());
        verify(pdfEventPort).publishPdfGenerated(any());
        verify(sagaReplyPort).publishSuccess(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq("http://minio/test.pdf"), eq(12345L));
    }

    @Test
    void completeGenerationAndPublish_appliesRetryCount() {
        UUID docId = UUID.randomUUID();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(docId).debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.findById(docId)).thenReturn(Optional.of(doc));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.completeGenerationAndPublish(
                docId, "2024/01/15/test.pdf", "http://minio/test.pdf", 12345L, 2,
                SAGA_ID, SAGA_STEP, CORRELATION_ID, DOCUMENT_ID, DOCUMENT_NUMBER);

        verify(documentArchivePort).publish(any());
        verify(pdfEventPort).publishPdfGenerated(any());
        verify(sagaReplyPort).publishSuccess(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq("http://minio/test.pdf"), eq(12345L));
    }

    @Test
    void failGenerationAndPublish_marksFailedAndPublishes() {
        UUID docId = UUID.randomUUID();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(docId).debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.findById(docId)).thenReturn(Optional.of(doc));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.failGenerationAndPublish(docId, "FOP crash", -1, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(sagaReplyPort).publishFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), eq("FOP crash"));
    }

    @Test
    void failGenerationAndPublish_handlesNullErrorMessage() {
        UUID docId = UUID.randomUUID();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(docId).debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.findById(docId)).thenReturn(Optional.of(doc));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.failGenerationAndPublish(docId, null, -1, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(sagaReplyPort).publishFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq("PDF generation failed"));
    }

    @Test
    void publishRetryExhausted_recordsMetricAndPublishesFailure() {
        service.publishRetryExhausted(SAGA_ID, SAGA_STEP, CORRELATION_ID, DOCUMENT_ID, DOCUMENT_NUMBER);

        verify(metrics).recordRetryExhausted(SAGA_ID, DOCUMENT_ID, DOCUMENT_NUMBER);
        verify(sagaReplyPort).publishFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq("Maximum retry attempts exceeded"));
    }

    @Test
    void publishIdempotentSuccess_rePublishesSuccess() {
        DebitCreditNotePdfDocument completedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.COMPLETED).documentUrl("http://minio/existing.pdf")
                .fileSize(9999L).build();

        service.publishIdempotentSuccess(completedDoc, SAGA_ID, SAGA_STEP, CORRELATION_ID, DOCUMENT_ID, DOCUMENT_NUMBER);

        verify(pdfEventPort).publishPdfGenerated(any());
        verify(sagaReplyPort).publishSuccess(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                eq("http://minio/existing.pdf"), eq(9999L));
    }

    @Test
    void publishGenerationFailure_publishesFailure() {
        service.publishGenerationFailure(SAGA_ID, SAGA_STEP, CORRELATION_ID, "some error");

        verify(sagaReplyPort).publishFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), eq("some error"));
    }

    @Test
    void publishCompensated_publishesCompensated() {
        service.publishCompensated(SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(sagaReplyPort).publishCompensated(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void publishCompensationFailure_publishesFailure() {
        service.publishCompensationFailure(SAGA_ID, SAGA_STEP, CORRELATION_ID, "comp error");

        verify(sagaReplyPort).publishFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), eq("comp error"));
    }

    @Test
    void deleteById_delegatesToRepository() {
        UUID docId = UUID.randomUUID();
        service.deleteById(docId);

        verify(repository).deleteById(docId);
        verify(repository).flush();
    }
}