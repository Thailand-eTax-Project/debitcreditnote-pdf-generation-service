package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;

import com.wpanther.debitcreditnote.pdf.domain.exception.DebitCreditNotePdfGenerationException;
import com.wpanther.debitcreditnote.pdf.domain.service.DebitCreditNotePdfGenerationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

@Service
@Slf4j
public class DebitCreditNotePdfGenerationServiceImpl implements DebitCreditNotePdfGenerationService {

    private static final String RSM_NS =
        "urn:etda:uncefact:data:standard:DebitCreditNote_CrossIndustryInvoice:2";
    private static final String RAM_NS =
        "urn:etda:uncefact:data:standard:DebitCreditNote_ReusableAggregateBusinessInformationEntity:2";
    private static final String GRAND_TOTAL_XPATH =
        "/rsm:DebitCreditNote_CrossIndustryInvoice" +
        "/rsm:SupplyChainTradeTransaction" +
        "/ram:ApplicableHeaderTradeSettlement" +
        "/ram:SpecifiedTradeSettlementHeaderMonetarySummation" +
        "/ram:GrandTotalAmount";

    private static final NamespaceContext NS_CONTEXT = new NamespaceContext() {
        @Override
        public String getNamespaceURI(String prefix) {
            return switch (prefix) {
                case "rsm" -> RSM_NS;
                case "ram" -> RAM_NS;
                default    -> XMLConstants.NULL_NS_URI;
            };
        }
        @Override public String getPrefix(String ns) { return null; }
        @Override public Iterator<String> getPrefixes(String ns) { return Collections.emptyIterator(); }
    };

    private final FopDebitCreditNotePdfGenerator fopPdfGenerator;
    private final PdfA3Converter pdfA3Converter;

    public DebitCreditNotePdfGenerationServiceImpl(FopDebitCreditNotePdfGenerator fopPdfGenerator,
                                                   PdfA3Converter pdfA3Converter) {
        this.fopPdfGenerator = fopPdfGenerator;
        this.pdfA3Converter  = pdfA3Converter;
    }

    @Override
    public byte[] generatePdf(String documentNumber, String signedXml)
            throws DebitCreditNotePdfGenerationException {

        log.info("Starting PDF generation for debit/credit note: {}", documentNumber);

        if (signedXml == null || signedXml.isBlank()) {
            throw new DebitCreditNotePdfGenerationException(
                "signedXml is null or blank for document: " + documentNumber);
        }

        try {
            BigDecimal grandTotal  = extractGrandTotal(signedXml, documentNumber);
            String amountInWords   = ThaiAmountWordsConverter.toWords(grandTotal);
            log.debug("Grand total {} → amountInWords: {}", grandTotal, amountInWords);

            Map<String, Object> params = Map.of("amountInWords", amountInWords);
            byte[] basePdf = fopPdfGenerator.generatePdf(signedXml, params);
            log.debug("Generated base PDF: {} bytes", basePdf.length);

            String xmlFilename = "debitcreditnote-" + documentNumber + ".xml";
            byte[] pdfA3 = pdfA3Converter.convertToPdfA3(basePdf, signedXml, xmlFilename, documentNumber);
            log.info("Generated PDF/A-3 for debit/credit note {}: {} bytes", documentNumber, pdfA3.length);
            return pdfA3;

        } catch (FopDebitCreditNotePdfGenerator.PdfGenerationException e) {
            throw new DebitCreditNotePdfGenerationException("PDF generation failed: " + e.getMessage(), e);
        } catch (PdfA3Converter.PdfConversionException e) {
            throw new DebitCreditNotePdfGenerationException("PDF/A-3 conversion failed: " + e.getMessage(), e);
        } catch (DebitCreditNotePdfGenerationException e) {
            throw e;
        } catch (Exception e) {
            throw new DebitCreditNotePdfGenerationException("PDF generation failed: " + e.getMessage(), e);
        }
    }

    private BigDecimal extractGrandTotal(String signedXml, String documentNumber)
            throws DebitCreditNotePdfGenerationException {
        try {
            XPath xpath = XPathFactory.newInstance().newXPath();
            xpath.setNamespaceContext(NS_CONTEXT);
            String value = (String) xpath.evaluate(
                GRAND_TOTAL_XPATH,
                new InputSource(new StringReader(signedXml)),
                XPathConstants.STRING);
            if (value == null || value.isBlank()) {
                throw new DebitCreditNotePdfGenerationException(
                    "GrandTotalAmount not found in signed XML for document: " + documentNumber);
            }
            return new BigDecimal(value.trim());
        } catch (XPathExpressionException e) {
            throw new DebitCreditNotePdfGenerationException(
                "Failed to extract GrandTotalAmount: " + e.getMessage(), e);
        } catch (NumberFormatException e) {
            throw new DebitCreditNotePdfGenerationException(
                "Invalid GrandTotalAmount for document " + documentNumber + ": " + e.getMessage(), e);
        }
    }
}
