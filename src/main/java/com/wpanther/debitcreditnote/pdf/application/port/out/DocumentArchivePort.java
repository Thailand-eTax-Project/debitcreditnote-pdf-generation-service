package com.wpanther.debitcreditnote.pdf.application.port.out;

import com.wpanther.debitcreditnote.pdf.application.dto.event.DocumentArchiveEvent;

public interface DocumentArchivePort {
    void publish(DocumentArchiveEvent event);
}