package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import org.springframework.stereotype.Component;

@Component
public class KafkaCommandMapper {

    public KafkaDebitCreditNoteProcessCommand toProcess(KafkaDebitCreditNoteProcessCommand src) {
        return src;
    }

    public KafkaDebitCreditNoteCompensateCommand toCompensate(KafkaDebitCreditNoteCompensateCommand src) {
        return src;
    }
}
