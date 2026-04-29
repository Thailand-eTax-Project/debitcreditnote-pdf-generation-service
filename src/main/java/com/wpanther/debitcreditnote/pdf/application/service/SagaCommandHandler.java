package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.debitcreditnote.pdf.application.usecase.CompensateDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.application.usecase.ProcessDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Orchestrates debit/credit note PDF generation in response to saga commands.
 * Full implementation in Task 11.
 */
@Service
@Slf4j
public class SagaCommandHandler implements ProcessDebitCreditNotePdfUseCase, CompensateDebitCreditNotePdfUseCase {

    @Override
    public void handle(KafkaDebitCreditNoteProcessCommand command) {
        log.info("Processing saga command for saga: {}, document: {}",
                command.getSagaId(), command.getDocumentNumber());
        // TODO: Full implementation in Task 11
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void handle(KafkaDebitCreditNoteCompensateCommand command) {
        log.info("Processing compensation for saga: {}, document: {}",
                command.getSagaId(), command.getDocumentId());
        // TODO: Full implementation in Task 11
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailure(KafkaDebitCreditNoteProcessCommand command, Throwable cause) {
        // TODO: Full implementation in Task 11
        log.error("Cannot notify orchestrator of DLQ failure for saga {}", command.getSagaId(), cause);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishCompensationOrchestrationFailure(KafkaDebitCreditNoteCompensateCommand command, Throwable cause) {
        // TODO: Full implementation in Task 11
        log.error("Cannot notify orchestrator of compensation DLQ failure for saga {}", command.getSagaId(), cause);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailureForUnparsedMessage(
            String sagaId, SagaStep sagaStep, String correlationId, Throwable cause) {
        // TODO: Full implementation in Task 11
        log.error("Cannot notify orchestrator of DLQ deserialization failure for saga {}", sagaId, cause);
    }
}
