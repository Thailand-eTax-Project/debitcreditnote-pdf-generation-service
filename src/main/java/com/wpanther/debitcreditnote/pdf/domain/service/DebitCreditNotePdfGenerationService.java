package com.wpanther.debitcreditnote.pdf.domain.service;

import com.wpanther.debitcreditnote.pdf.domain.exception.DebitCreditNotePdfGenerationException;

/**
 * Domain service port for debit/credit note PDF generation.
 */
public interface DebitCreditNotePdfGenerationService {

    /**
     * Generate PDF/A-3 from the signed XML document.
     *
     * @param documentNumber document number (used for logging and file naming)
     * @param signedXml      full Thai e-Tax signed XML
     * @return PDF/A-3 bytes with the signed XML embedded as an attachment
     * @throws DebitCreditNotePdfGenerationException if generation fails
     */
    byte[] generatePdf(String documentNumber, String signedXml)
        throws DebitCreditNotePdfGenerationException;
}
