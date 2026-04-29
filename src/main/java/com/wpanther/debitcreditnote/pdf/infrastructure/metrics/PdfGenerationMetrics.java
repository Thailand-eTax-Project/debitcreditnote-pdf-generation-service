package com.wpanther.debitcreditnote.pdf.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class PdfGenerationMetrics {

    private static final String RETRY_EXHAUSTED_COUNTER = "pdf.generation.retry.exhausted";
    private static final String TAG_SAGA_ID             = "saga_id";
    private static final String TAG_DCN_ID              = "debit_credit_note_id";
    private static final String TAG_DCN_NUMBER          = "document_number";

    private final MeterRegistry meterRegistry;

    public PdfGenerationMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        Counter.builder(RETRY_EXHAUSTED_COUNTER)
                .description("Number of times PDF generation max retries were exceeded")
                .register(meterRegistry);
    }

    public void recordRetryExhausted(String sagaId, String debitCreditNoteId, String documentNumber) {
        meterRegistry.counter(RETRY_EXHAUSTED_COUNTER,
                TAG_SAGA_ID,    sagaId,
                TAG_DCN_ID,     debitCreditNoteId,
                TAG_DCN_NUMBER, documentNumber)
            .increment();
    }
}
