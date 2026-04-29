package com.wpanther.debitcreditnote.pdf.application.port.out;

import com.wpanther.debitcreditnote.pdf.application.event.DebitCreditNotePdfGeneratedEvent;

public interface PdfEventPort {
    void publishPdfGenerated(DebitCreditNotePdfGeneratedEvent event);
}
