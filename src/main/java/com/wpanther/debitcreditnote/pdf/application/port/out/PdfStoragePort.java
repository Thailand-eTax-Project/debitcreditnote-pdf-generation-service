package com.wpanther.debitcreditnote.pdf.application.port.out;

public interface PdfStoragePort {
    /** Upload PDF bytes and return the S3 key. */
    String store(String documentNumber, byte[] pdfBytes);
    /** Delete a stored PDF by S3 key (best-effort). */
    void delete(String s3Key);
    /** Resolve a full URL from an S3 key. */
    String resolveUrl(String s3Key);
}
