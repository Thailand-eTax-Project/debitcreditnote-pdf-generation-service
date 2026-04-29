package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
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
    @Mock private PdfGenerationMetrics metrics;
    @InjectMocks private DebitCreditNotePdfDocumentService service;

    private KafkaDebitCreditNoteProcessCommand command() {
        return new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", "DCN-2024-001", "http://storage/signed.xml");
    }

    private KafkaDebitCreditNoteCompensateCommand compensateCommand() {
        return new KafkaDebitCreditNoteCompensateCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1", "dcn-001");
    }

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
                docId, "2024/01/15/test.pdf", "http://minio/test.pdf", 12345L, -1, command());

        verify(pdfEventPort).publishPdfGenerated(any());
        verify(sagaReplyPort).publishSuccess(eq("saga-1"), any(), eq("corr-1"),
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
                docId, "2024/01/15/test.pdf", "http://minio/test.pdf", 12345L, 2, command());

        verify(pdfEventPort).publishPdfGenerated(any());
        verify(sagaReplyPort).publishSuccess(eq("saga-1"), any(), eq("corr-1"),
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

        service.failGenerationAndPublish(docId, "FOP crash", -1, command());

        verify(sagaReplyPort).publishFailure(eq("saga-1"), any(), eq("corr-1"), eq("FOP crash"));
    }

    @Test
    void failGenerationAndPublish_handlesNullErrorMessage() {
        UUID docId = UUID.randomUUID();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(docId).debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.findById(docId)).thenReturn(Optional.of(doc));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.failGenerationAndPublish(docId, null, -1, command());

        verify(sagaReplyPort).publishFailure(eq("saga-1"), any(), eq("corr-1"),
                eq("PDF generation failed"));
    }

    @Test
    void publishRetryExhausted_recordsMetricAndPublishesFailure() {
        service.publishRetryExhausted(command());

        verify(metrics).recordRetryExhausted("saga-1", "dcn-001", "DCN-2024-001");
        verify(sagaReplyPort).publishFailure(eq("saga-1"), any(), eq("corr-1"),
                eq("Maximum retry attempts exceeded"));
    }

    @Test
    void publishIdempotentSuccess_rePublishesSuccess() {
        DebitCreditNotePdfDocument completedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.COMPLETED).documentUrl("http://minio/existing.pdf")
                .fileSize(9999L).build();

        service.publishIdempotentSuccess(completedDoc, command());

        verify(pdfEventPort).publishPdfGenerated(any());
        verify(sagaReplyPort).publishSuccess(eq("saga-1"), any(), eq("corr-1"),
                eq("http://minio/existing.pdf"), eq(9999L));
    }

    @Test
    void publishGenerationFailure_publishesFailure() {
        service.publishGenerationFailure(command(), "some error");

        verify(sagaReplyPort).publishFailure(eq("saga-1"), any(), eq("corr-1"), eq("some error"));
    }

    @Test
    void publishCompensated_publishesCompensated() {
        service.publishCompensated(compensateCommand());

        verify(sagaReplyPort).publishCompensated(eq("saga-1"), any(), eq("corr-1"));
    }

    @Test
    void publishCompensationFailure_publishesFailure() {
        service.publishCompensationFailure(compensateCommand(), "comp error");

        verify(sagaReplyPort).publishFailure(eq("saga-1"), any(), eq("corr-1"), eq("comp error"));
    }

    @Test
    void deleteById_delegatesToRepository() {
        UUID docId = UUID.randomUUID();
        service.deleteById(docId);

        verify(repository).deleteById(docId);
        verify(repository).flush();
    }
}
