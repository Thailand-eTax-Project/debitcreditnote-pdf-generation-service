package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.saga.domain.enums.SagaStep;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaCommandMapperTest {

    private final KafkaCommandMapper mapper = new KafkaCommandMapper();

    @Test
    void toProcess_returnsSameInstance() {
        var cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "doc-1", "DCN-2024-001", "http://storage/signed.xml");
        assertThat(mapper.toProcess(cmd)).isSameAs(cmd);
    }

    @Test
    void toCompensate_returnsSameInstance() {
        var cmd = new KafkaDebitCreditNoteCompensateCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1", "doc-1");
        assertThat(mapper.toCompensate(cmd)).isSameAs(cmd);
    }

    @Test
    void processCommand_fieldsPreserved() {
        var cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "doc-1", "DCN-2024-001", "http://storage/signed.xml");
        assertThat(cmd.getSagaId()).isEqualTo("saga-1");
        assertThat(cmd.getDocumentId()).isEqualTo("doc-1");
        assertThat(cmd.getDocumentNumber()).isEqualTo("DCN-2024-001");
        assertThat(cmd.getSignedXmlUrl()).isEqualTo("http://storage/signed.xml");
        assertThat(cmd.getSagaStep()).isEqualTo(SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF);
    }

    @Test
    void compensateCommand_fieldsPreserved() {
        var cmd = new KafkaDebitCreditNoteCompensateCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1", "doc-1");
        assertThat(cmd.getSagaId()).isEqualTo("saga-1");
        assertThat(cmd.getDocumentId()).isEqualTo("doc-1");
        assertThat(cmd.getCorrelationId()).isEqualTo("corr-1");
    }
}
