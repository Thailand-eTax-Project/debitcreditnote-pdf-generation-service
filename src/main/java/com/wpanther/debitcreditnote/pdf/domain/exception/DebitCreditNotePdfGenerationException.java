package com.wpanther.debitcreditnote.pdf.domain.exception;

public class DebitCreditNotePdfGenerationException extends RuntimeException {

    public DebitCreditNotePdfGenerationException(String message) {
        super(message);
    }

    public DebitCreditNotePdfGenerationException(String message, Throwable cause) {
        super(message, cause);
    }
}
