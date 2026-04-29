package com.wpanther.debitcreditnote.pdf.domain.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DebitCreditNotePdfGenerationExceptionTest {

    @Test
    void messageConstructor() {
        var ex = new DebitCreditNotePdfGenerationException("error");
        assertThat(ex.getMessage()).isEqualTo("error");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void messageCauseConstructor() {
        var cause = new RuntimeException("root");
        var ex = new DebitCreditNotePdfGenerationException("error", cause);
        assertThat(ex.getMessage()).isEqualTo("error");
        assertThat(ex.getCause()).isSameAs(cause);
    }
}
