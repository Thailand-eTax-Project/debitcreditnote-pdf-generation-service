package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

@DataJpaTest
@ActiveProfiles("test")
@Import(DebitCreditNotePdfDocumentRepositoryAdapter.class)
class JpaDebitCreditNotePdfDocumentRepositoryImplTest {

    @Autowired
    private DebitCreditNotePdfDocumentRepository repository;

    @Test
    void saveAndFindById() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-2024-001")
                .build();
        DebitCreditNotePdfDocument saved = repository.save(doc);

        Optional<DebitCreditNotePdfDocument> found = repository.findById(saved.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getDebitCreditNoteId()).isEqualTo("dcn-001");
        assertThat(found.get().getStatus()).isEqualTo(GenerationStatus.PENDING);
    }

    @Test
    void findByDebitCreditNoteId_found() {
        repository.save(DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-002").documentNumber("DCN-2024-002").build());

        Optional<DebitCreditNotePdfDocument> found = repository.findByDebitCreditNoteId("dcn-002");
        assertThat(found).isPresent();
        assertThat(found.get().getDocumentNumber()).isEqualTo("DCN-2024-002");
    }

    @Test
    void findByDebitCreditNoteId_notFound() {
        assertThat(repository.findByDebitCreditNoteId("nonexistent")).isEmpty();
    }

    @Test
    void deleteById_removesDocument() {
        DebitCreditNotePdfDocument doc = repository.save(
                DebitCreditNotePdfDocument.builder()
                        .debitCreditNoteId("dcn-003").documentNumber("DCN-2024-003").build());
        repository.deleteById(doc.getId());
        assertThat(repository.findById(doc.getId())).isEmpty();
    }
}
