package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.port.out.PdfStoragePort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.service.DebitCreditNotePdfGenerationService;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
import com.wpanther.saga.domain.enums.SagaStep;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SagaCommandHandlerTest {

    @Mock private DebitCreditNotePdfDocumentService pdfDocumentService;
    @Mock private DebitCreditNotePdfGenerationService pdfGenerationService;
    @Mock private PdfStoragePort pdfStoragePort;
    @Mock private SagaReplyPort sagaReplyPort;
    @Mock private SignedXmlFetchPort signedXmlFetchPort;

    private SagaCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new SagaCommandHandler(
                pdfDocumentService, pdfGenerationService,
                pdfStoragePort, sagaReplyPort, signedXmlFetchPort, 3);
    }

    private KafkaDebitCreditNoteProcessCommand processCommand() {
        return new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", "DCN-2024-001", "http://storage/signed.xml");
    }

    private KafkaDebitCreditNoteCompensateCommand compensateCommand() {
        return new KafkaDebitCreditNoteCompensateCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1", "dcn-001");
    }

    private DebitCreditNotePdfDocument generatingDoc() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001").build();
        doc.startGeneration();
        return doc;
    }

    @Test
    void handle_processCommand_success() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        when(signedXmlFetchPort.fetch("http://storage/signed.xml")).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf("DCN-2024-001", "<xml/>")).thenReturn(new byte[100]);
        when(pdfStoragePort.store("DCN-2024-001", new byte[100])).thenReturn("2024/01/15/test.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/test.pdf")).thenReturn("http://minio/test.pdf");

        handler.handle(processCommand());

        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(doc.getId()), eq("2024/01/15/test.pdf"), eq("http://minio/test.pdf"),
                eq(100L), eq(-1), any());
    }

    @Test
    void handle_processCommand_idempotentSuccess() {
        DebitCreditNotePdfDocument completedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.COMPLETED).documentUrl("http://minio/existing.pdf")
                .fileSize(9999L).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001"))
                .thenReturn(Optional.of(completedDoc));

        handler.handle(processCommand());

        verify(pdfDocumentService).publishIdempotentSuccess(eq(completedDoc), any());
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_processCommand_maxRetriesExceeded() {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.FAILED).retryCount(3).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001"))
                .thenReturn(Optional.of(failedDoc));

        handler.handle(processCommand());

        verify(pdfDocumentService).publishRetryExhausted(any());
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_processCommand_generationFailure() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString()))
                .thenThrow(new RuntimeException("FOP failed"));

        handler.handle(processCommand());

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("FOP failed"), eq(-1), any());
    }

    @Test
    void handle_compensateCommand_success() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .documentPath("2024/01/15/test.pdf").status(GenerationStatus.COMPLETED).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.of(doc));

        handler.handle(compensateCommand());

        verify(pdfDocumentService).deleteById(doc.getId());
        verify(pdfStoragePort).delete("2024/01/15/test.pdf");
        verify(pdfDocumentService).publishCompensated(any());
    }

    @Test
    void handle_compensateCommand_idempotent() {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());

        handler.handle(compensateCommand());

        verify(pdfDocumentService, never()).deleteById(any());
        verify(pdfDocumentService).publishCompensated(any());
    }

    @Test
    void handle_processCommand_nullSignedXmlUrl() {
        KafkaDebitCreditNoteProcessCommand cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", "DCN-2024-001", null);

        handler.handle(cmd);

        verify(pdfDocumentService).publishGenerationFailure(any(), contains("signedXmlUrl"));
    }

    @Test
    void handle_processCommand_blankSignedXmlUrl() {
        KafkaDebitCreditNoteProcessCommand cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", "DCN-2024-001", "   ");

        handler.handle(cmd);

        verify(pdfDocumentService).publishGenerationFailure(any(), contains("signedXmlUrl"));
    }

    @Test
    void handle_processCommand_nullDocumentId() {
        KafkaDebitCreditNoteProcessCommand cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                null, "DCN-2024-001", "http://storage/signed.xml");

        handler.handle(cmd);

        verify(pdfDocumentService).publishGenerationFailure(any(), contains("documentId"));
    }

    @Test
    void handle_processCommand_nullDocumentNumber() {
        KafkaDebitCreditNoteProcessCommand cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", null, "http://storage/signed.xml");

        handler.handle(cmd);

        verify(pdfDocumentService).publishGenerationFailure(any(), contains("documentNumber"));
    }

    @Test
    void handle_compensateCommand_compensateFailure_deletesFromStorage() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .documentPath("2024/01/15/test.pdf").status(GenerationStatus.COMPLETED).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.of(doc));
        doThrow(new RuntimeException("DB error")).when(pdfDocumentService).deleteById(doc.getId());

        handler.handle(compensateCommand());

        verify(pdfDocumentService).publishCompensationFailure(any(), contains("Compensation failed"));
    }

    @Test
    void handle_processCommand_retryScenario() throws Exception {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.FAILED).retryCount(1).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.of(failedDoc));

        DebitCreditNotePdfDocument newDoc = generatingDoc();
        when(pdfDocumentService.replaceAndBeginGeneration(eq(failedDoc.getId()), eq(1),
                eq("dcn-001"), eq("DCN-2024-001"))).thenReturn(newDoc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/retry.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/retry.pdf")).thenReturn("http://minio/retry.pdf");

        handler.handle(processCommand());

        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(newDoc.getId()), eq("2024/01/15/retry.pdf"), eq("http://minio/retry.pdf"),
                eq(50L), eq(1), any());
    }

    @Test
    void handle_processCommand_circuitBreakerOpen() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        CircuitBreaker cb = CircuitBreaker.of("test", CircuitBreakerConfig.ofDefaults());
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(CallNotPermittedException.createCallNotPermittedException(cb));

        handler.handle(processCommand());

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("Circuit breaker open"), eq(-1), any());
    }

    @Test
    void handle_processCommand_httpClientError() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(new HttpClientErrorException(
                        org.springframework.http.HttpStatus.NOT_FOUND, "Not Found"));

        handler.handle(processCommand());

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("HTTP error fetching signed XML"), eq(-1), any());
    }

    @Test
    void handle_processCommand_httpServerError() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(new HttpServerErrorException(
                        org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR));

        handler.handle(processCommand());

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("HTTP error fetching signed XML"), eq(-1), any());
    }

    @Test
    void handle_processCommand_pdfGenerationFailure_cleansUpOrphanPdf() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/orphan.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/orphan.pdf"))
                .thenThrow(new RuntimeException("URL resolution failed"));
        doNothing().when(pdfStoragePort).delete(anyString());

        handler.handle(processCommand());

        verify(pdfStoragePort).delete("2024/01/15/orphan.pdf");
        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("URL resolution failed"), eq(-1), any());
    }

    @Test
    void publishOrchestrationFailure_notifiesOrchestrator() {
        KafkaDebitCreditNoteProcessCommand cmd = processCommand();
        handler.publishOrchestrationFailure(cmd, new RuntimeException("DLQ reason"));

        verify(sagaReplyPort).publishFailure(
                eq("saga-1"), eq(SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF), eq("corr-1"),
                contains("DLQ reason"));
    }

    @Test
    void publishCompensationOrchestrationFailure_notifiesOrchestrator() {
        KafkaDebitCreditNoteCompensateCommand cmd = compensateCommand();
        handler.publishCompensationOrchestrationFailure(cmd, new RuntimeException("Comp DLQ"));

        verify(sagaReplyPort).publishFailure(
                eq("saga-1"), eq(SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF), eq("corr-1"),
                contains("Compensation DLQ"));
    }

    @Test
    void publishOrchestrationFailureForUnparsedMessage_notifiesOrchestrator() {
        handler.publishOrchestrationFailureForUnparsedMessage(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                new RuntimeException("deserialization failed"));

        verify(sagaReplyPort).publishFailure(
                eq("saga-1"), eq(SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF), eq("corr-1"),
                contains("deserialization failure"));
    }

    @Test
    void handle_processCommand_withNoRetryNotMaxRetriesExceeded() throws Exception {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.FAILED).retryCount(1).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.of(failedDoc));

        DebitCreditNotePdfDocument newDoc = generatingDoc();
        when(pdfDocumentService.replaceAndBeginGeneration(eq(failedDoc.getId()), eq(1),
                eq("dcn-001"), eq("DCN-2024-001"))).thenReturn(newDoc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/retry.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/retry.pdf")).thenReturn("http://minio/retry.pdf");

        handler.handle(processCommand());

        verify(pdfDocumentService, never()).publishRetryExhausted(any());
    }
}
