package com.wpanther.debitcreditnote.pdf.domain.model;

import com.wpanther.debitcreditnote.pdf.domain.exception.DebitCreditNotePdfGenerationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@DisplayName("DebitCreditNotePdfDocument Aggregate Tests")
class DebitCreditNotePdfDocumentTest {

    private DebitCreditNotePdfDocument pendingDocument() {
        return DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-2024-001")
                .build();
    }

    // -------------------------------------------------------------------------
    // Builder / invariants
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Should create document in PENDING status with defaults")
    void testCreate_Defaults() {
        DebitCreditNotePdfDocument doc = pendingDocument();

        assertThat(doc.getId()).isNotNull();
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.PENDING);
        assertThat(doc.getMimeType()).isEqualTo("application/pdf");
        assertThat(doc.getRetryCount()).isZero();
        assertThat(doc.isXmlEmbedded()).isFalse();
        assertThat(doc.getCreatedAt()).isNotNull();
    }

    @Test
    @DisplayName("Should reject blank debitCreditNoteId")
    void testCreate_BlankDebitCreditNoteId() {
        assertThatThrownBy(() ->
                DebitCreditNotePdfDocument.builder()
                        .debitCreditNoteId("   ")
                        .documentNumber("DCN-001")
                        .build()
        ).isInstanceOf(DebitCreditNotePdfGenerationException.class)
         .hasMessageContaining("Debit/Credit Note ID cannot be blank");
    }

    @Test
    @DisplayName("Should reject null documentNumber")
    void testCreate_NullDocumentNumber() {
        assertThatThrownBy(() ->
                DebitCreditNotePdfDocument.builder()
                        .debitCreditNoteId("dcn-001")
                        .documentNumber(null)
                        .build()
        ).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should reject blank documentNumber")
    void testCreate_BlankDocumentNumber() {
        assertThatThrownBy(() ->
                DebitCreditNotePdfDocument.builder()
                        .debitCreditNoteId("dcn-001")
                        .documentNumber("   ")
                        .build()
        ).isInstanceOf(DebitCreditNotePdfGenerationException.class)
         .hasMessageContaining("Document number cannot be blank");
    }

    // -------------------------------------------------------------------------
    // State machine — happy path
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("PENDING -> startGeneration() -> GENERATING")
    void testStartGeneration() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.GENERATING);
    }

    @Test
    @DisplayName("GENERATING -> markCompleted() -> COMPLETED")
    void testMarkCompleted() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();
        doc.markCompleted("2024/01/15/test.pdf", "http://minio/test.pdf", 12345L);

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
        assertThat(doc.getDocumentPath()).isEqualTo("2024/01/15/test.pdf");
        assertThat(doc.getDocumentUrl()).isEqualTo("http://minio/test.pdf");
        assertThat(doc.getFileSize()).isEqualTo(12345L);
        assertThat(doc.getCompletedAt()).isNotNull();
        assertThat(doc.isCompleted()).isTrue();
        assertThat(doc.isSuccessful()).isTrue();
    }

    @Test
    @DisplayName("Any state -> markFailed() -> FAILED")
    void testMarkFailed_FromPending() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.markFailed("Something went wrong");

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.FAILED);
        assertThat(doc.getErrorMessage()).isEqualTo("Something went wrong");
        assertThat(doc.isFailed()).isTrue();
        assertThat(doc.isCompleted()).isFalse();
        assertThat(doc.getCompletedAt()).isNotNull();
    }

    @Test
    @DisplayName("PENDING -> markXmlEmbedded() sets flag")
    void testMarkXmlEmbedded() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        assertThat(doc.isXmlEmbedded()).isFalse();
        doc.markXmlEmbedded();
        assertThat(doc.isXmlEmbedded()).isTrue();
    }

    // -------------------------------------------------------------------------
    // State machine — invalid transitions
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("startGeneration() from GENERATING throws exception")
    void testStartGeneration_AlreadyGenerating() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(doc::startGeneration)
                .isInstanceOf(DebitCreditNotePdfGenerationException.class)
                .hasMessageContaining("PENDING");
    }

    @Test
    @DisplayName("markCompleted() from PENDING throws exception")
    void testMarkCompleted_FromPending() {
        DebitCreditNotePdfDocument doc = pendingDocument();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 100L))
                .isInstanceOf(DebitCreditNotePdfGenerationException.class)
                .hasMessageContaining("GENERATING");
    }

    @Test
    @DisplayName("markCompleted() with zero fileSize throws IllegalArgumentException")
    void testMarkCompleted_ZeroFileSize() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("File size must be positive");
    }

    @Test
    @DisplayName("markCompleted() with null documentPath throws NullPointerException")
    void testMarkCompleted_NullPath() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted(null, "url", 100L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("markCompleted() with null documentUrl throws NullPointerException")
    void testMarkCompleted_NullUrl() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted("path", null, 100L))
                .isInstanceOf(NullPointerException.class);
    }

    // -------------------------------------------------------------------------
    // Retry tracking
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("incrementRetryCount() increases count by one")
    void testIncrementRetryCount() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        assertThat(doc.getRetryCount()).isZero();
        doc.incrementRetryCount();
        assertThat(doc.getRetryCount()).isOne();
        doc.incrementRetryCount();
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("incrementRetryCountTo() advances count to target")
    void testIncrementRetryCountTo_AdvancesToTarget() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.incrementRetryCountTo(2);
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("incrementRetryCountTo() is a no-op when count already higher")
    void testIncrementRetryCountTo_NoOpWhenAlreadyHigher() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .retryCount(2)
                .build();
        doc.incrementRetryCountTo(1);
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("isMaxRetriesExceeded() returns true when retryCount >= maxRetries")
    void testIsMaxRetriesExceeded_AtLimit() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .retryCount(3)
                .build();
        assertThat(doc.isMaxRetriesExceeded(3)).isTrue();
    }

    @Test
    @DisplayName("isMaxRetriesExceeded() returns false when retryCount < maxRetries")
    void testIsMaxRetriesExceeded_BelowLimit() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .retryCount(2)
                .build();
        assertThat(doc.isMaxRetriesExceeded(3)).isFalse();
    }

    // -------------------------------------------------------------------------
    // equals / hashCode / toString
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("equals and hashCode based on id")
    void testEqualsHashCode() {
        DebitCreditNotePdfDocument doc1 = DebitCreditNotePdfDocument.builder()
                .id(java.util.UUID.fromString("00000000-0000-0000-0000-000000000001"))
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .build();
        DebitCreditNotePdfDocument doc2 = DebitCreditNotePdfDocument.builder()
                .id(java.util.UUID.fromString("00000000-0000-0000-0000-000000000001"))
                .debitCreditNoteId("dcn-002")
                .documentNumber("DCN-002")
                .build();
        DebitCreditNotePdfDocument doc3 = DebitCreditNotePdfDocument.builder()
                .id(java.util.UUID.fromString("00000000-0000-0000-0000-000000000002"))
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .build();

        assertThat(doc1).isEqualTo(doc2);
        assertThat(doc1.hashCode()).isEqualTo(doc2.hashCode());
        assertThat(doc1).isNotEqualTo(doc3);
    }

    @Test
    @DisplayName("toString contains key fields")
    void testToString() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        String str = doc.toString();
        assertThat(str).contains("DebitCreditNotePdfDocument");
        assertThat(str).contains("dcn-001");
        assertThat(str).contains("DCN-2024-001");
        assertThat(str).contains("PENDING");
    }
}
