package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.port.out.PdfStoragePort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.service.DebitCreditNotePdfGenerationService;
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

    private com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.SagaCommandHandler handler;

    private static final String SAGA_ID = "saga-1";
    private static final SagaStep SAGA_STEP = SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF;
    private static final String CORRELATION_ID = "corr-1";
    private static final String DOCUMENT_ID = "dcn-001";
    private static final String DOCUMENT_NUMBER = "DCN-2024-001";
    private static final String SIGNED_XML_URL = "http://storage/signed.xml";

    @BeforeEach
    void setUp() {
        handler = new com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.SagaCommandHandler(
                pdfDocumentService, pdfGenerationService,
                pdfStoragePort, sagaReplyPort, signedXmlFetchPort, 3);
    }

    private DebitCreditNotePdfDocument generatingDoc() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER).build();
        doc.startGeneration();
        return doc;
    }

    @Test
    void handle_processCommand_success() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(SIGNED_XML_URL)).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(DOCUMENT_NUMBER, "<xml/>")).thenReturn(new byte[100]);
        when(pdfStoragePort.store(DOCUMENT_NUMBER, new byte[100])).thenReturn("2024/01/15/test.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/test.pdf")).thenReturn("http://minio/test.pdf");

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(doc.getId()), eq("2024/01/15/test.pdf"), eq("http://minio/test.pdf"),
                eq(100L), eq(-1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
    }

    @Test
    void handle_processCommand_idempotentSuccess() {
        DebitCreditNotePdfDocument completedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.COMPLETED).documentUrl("http://minio/existing.pdf")
                .fileSize(9999L).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID))
                .thenReturn(Optional.of(completedDoc));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishIdempotentSuccess(eq(completedDoc), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_processCommand_maxRetriesExceeded() {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.FAILED).retryCount(3).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID))
                .thenReturn(Optional.of(failedDoc));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishRetryExhausted(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_processCommand_generationFailure() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString()))
                .thenThrow(new RuntimeException("FOP failed"));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("FOP failed"), eq(-1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_compensateCommand_success() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .documentPath("2024/01/15/test.pdf").status(GenerationStatus.COMPLETED).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(doc));

        handler.handle(DOCUMENT_ID, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).deleteById(doc.getId());
        verify(pdfStoragePort).delete("2024/01/15/test.pdf");
        verify(pdfDocumentService).publishCompensated(SAGA_ID, SAGA_STEP, CORRELATION_ID);
    }

    @Test
    void handle_compensateCommand_idempotent() {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());

        handler.handle(DOCUMENT_ID, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService, never()).deleteById(any());
        verify(pdfDocumentService).publishCompensated(SAGA_ID, SAGA_STEP, CORRELATION_ID);
    }

    @Test
    void handle_processCommand_nullSignedXmlUrl() {
        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, null, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("signedXmlUrl"));
    }

    @Test
    void handle_processCommand_blankSignedXmlUrl() {
        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, "   ", SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("signedXmlUrl"));
    }

    @Test
    void handle_processCommand_nullDocumentId() {
        handler.handle(null, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("documentId"));
    }

    @Test
    void handle_processCommand_nullDocumentNumber() {
        handler.handle(DOCUMENT_ID, null, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishGenerationFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("documentNumber"));
    }

    @Test
    void handle_compensateCommand_compensateFailure_deletesFromStorage() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .documentPath("2024/01/15/test.pdf").status(GenerationStatus.COMPLETED).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(doc));
        doThrow(new RuntimeException("DB error")).when(pdfDocumentService).deleteById(doc.getId());

        handler.handle(DOCUMENT_ID, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).publishCompensationFailure(eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), contains("Compensation failed"));
    }

    @Test
    void handle_processCommand_retryScenario() throws Exception {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.FAILED).retryCount(1).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(failedDoc));

        DebitCreditNotePdfDocument newDoc = generatingDoc();
        when(pdfDocumentService.replaceAndBeginGeneration(eq(failedDoc.getId()), eq(1),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER))).thenReturn(newDoc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/retry.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/retry.pdf")).thenReturn("http://minio/retry.pdf");

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(newDoc.getId()), eq("2024/01/15/retry.pdf"), eq("http://minio/retry.pdf"),
                eq(50L), eq(1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID), eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER));
    }

    @Test
    void handle_processCommand_circuitBreakerOpen() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        CircuitBreaker cb = CircuitBreaker.of("test", CircuitBreakerConfig.ofDefaults());
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(CallNotPermittedException.createCallNotPermittedException(cb));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("Circuit breaker open"), eq(-1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_processCommand_httpClientError() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(new HttpClientErrorException(
                        org.springframework.http.HttpStatus.NOT_FOUND, "Not Found"));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("HTTP error fetching signed XML"), eq(-1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_processCommand_httpServerError() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString()))
                .thenThrow(new HttpServerErrorException(
                        org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR));

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("HTTP error fetching signed XML"), eq(-1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void handle_processCommand_pdfGenerationFailure_cleansUpOrphanPdf() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration(DOCUMENT_ID, DOCUMENT_NUMBER)).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/orphan.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/orphan.pdf"))
                .thenThrow(new RuntimeException("URL resolution failed"));
        doNothing().when(pdfStoragePort).delete(anyString());

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfStoragePort).delete("2024/01/15/orphan.pdf");
        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("URL resolution failed"), eq(-1), eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID));
    }

    @Test
    void publishOrchestrationFailure_notifiesOrchestrator() {
        com.wpanther.saga.domain.model.SagaCommand cmd =
                new com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCommand(
                        SAGA_ID, SAGA_STEP, CORRELATION_ID, DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL);
        handler.publishOrchestrationFailure(cmd, new RuntimeException("DLQ reason"));

        verify(sagaReplyPort).publishFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                contains("DLQ reason"));
    }

    @Test
    void publishCompensationOrchestrationFailure_notifiesOrchestrator() {
        com.wpanther.saga.domain.model.SagaCommand cmd =
                new com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.dto.DebitCreditNotePdfCompensationCommand(
                        SAGA_ID, SAGA_STEP, CORRELATION_ID, DOCUMENT_ID);
        handler.publishCompensationOrchestrationFailure(cmd, new RuntimeException("Comp DLQ"));

        verify(sagaReplyPort).publishFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                contains("Compensation DLQ"));
    }

    @Test
    void publishOrchestrationFailureForUnparsedMessage_notifiesOrchestrator() {
        handler.publishOrchestrationFailureForUnparsedMessage(
                SAGA_ID, SAGA_STEP, CORRELATION_ID,
                new RuntimeException("deserialization failed"));

        verify(sagaReplyPort).publishFailure(
                eq(SAGA_ID), eq(SAGA_STEP), eq(CORRELATION_ID),
                contains("deserialization failure"));
    }

    @Test
    void handle_processCommand_withNoRetryNotMaxRetriesExceeded() throws Exception {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(DOCUMENT_ID).documentNumber(DOCUMENT_NUMBER)
                .status(GenerationStatus.FAILED).retryCount(1).build();
        when(pdfDocumentService.findByDebitCreditNoteId(DOCUMENT_ID)).thenReturn(Optional.of(failedDoc));

        DebitCreditNotePdfDocument newDoc = generatingDoc();
        when(pdfDocumentService.replaceAndBeginGeneration(eq(failedDoc.getId()), eq(1),
                eq(DOCUMENT_ID), eq(DOCUMENT_NUMBER))).thenReturn(newDoc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(new byte[50]);
        when(pdfStoragePort.store(anyString(), any(byte[].class))).thenReturn("2024/01/15/retry.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/retry.pdf")).thenReturn("http://minio/retry.pdf");

        handler.handle(DOCUMENT_ID, DOCUMENT_NUMBER, SIGNED_XML_URL, SAGA_ID, SAGA_STEP, CORRELATION_ID);

        verify(pdfDocumentService, never()).publishRetryExhausted(any(), any(), any(), any(), any());
    }
}