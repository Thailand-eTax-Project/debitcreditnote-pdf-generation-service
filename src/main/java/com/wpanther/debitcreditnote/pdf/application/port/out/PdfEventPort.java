package com.wpanther.debitcreditnote.pdf.application.port.out;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging.DebitCreditNotePdfGeneratedEvent;

public interface PdfEventPort {
    void publishPdfGenerated(DebitCreditNotePdfGeneratedEvent event);
}
