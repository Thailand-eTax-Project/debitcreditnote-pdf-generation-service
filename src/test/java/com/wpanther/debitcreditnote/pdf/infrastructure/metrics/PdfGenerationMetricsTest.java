package com.wpanther.debitcreditnote.pdf.infrastructure.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class PdfGenerationMetricsTest {

    @Test
    void constructor_registersCounter() {
        assertThatCode(() -> new PdfGenerationMetrics(new SimpleMeterRegistry()))
                .doesNotThrowAnyException();
    }

    @Test
    void recordRetryExhausted_incrementsCounter() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        PdfGenerationMetrics metrics = new PdfGenerationMetrics(meterRegistry);

        metrics.recordRetryExhausted("saga-1", "dcn-001", "DCN-2024-001");

        double count = meterRegistry.counter("pdf.generation.retry.exhausted",
                "saga_id", "saga-1",
                "debit_credit_note_id", "dcn-001",
                "document_number", "DCN-2024-001").count();
        assertThat(count).isEqualTo(1.0);
    }

    @Test
    void recordRetryExhausted_multipleCalls_accumulate() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        PdfGenerationMetrics metrics = new PdfGenerationMetrics(meterRegistry);

        metrics.recordRetryExhausted("saga-1", "dcn-001", "DCN-2024-001");
        metrics.recordRetryExhausted("saga-2", "dcn-002", "DCN-2024-002");

        double count = meterRegistry.get("pdf.generation.retry.exhausted").counters().stream()
                .mapToDouble(c -> c.count())
                .sum();
        assertThat(count).isEqualTo(2.0);
    }
}
