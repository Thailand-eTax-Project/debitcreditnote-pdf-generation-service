package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.tracing.annotation.NewSpan;
import lombok.extern.slf4j.Slf4j;
import org.apache.fop.apps.Fop;
import org.apache.fop.apps.FopFactory;
import org.apache.fop.apps.MimeConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class FopDebitCreditNotePdfGenerator {

    private static final String FOP_CONFIG_PATH         = "fop/fop.xconf";
    private static final String DEBITCREDITNOTE_XSL_PATH = "xsl/debitcreditnote-direct.xsl";

    private static final List<String> REQUIRED_FONTS = List.of(
            "fonts/THSarabunNew.ttf",
            "fonts/THSarabunNew-Bold.ttf"
    );

    private final FopFactory fopFactory;
    private final Templates cachedTemplates;
    private final Semaphore renderSemaphore;
    private final Timer renderTimer;
    private final DistributionSummary pdfSizeSummary;
    private final long maxPdfSizeBytes;

    public FopDebitCreditNotePdfGenerator(
            @Value("${app.pdf.generation.max-concurrent-renders:3}") int maxConcurrentRenders,
            @Value("${app.pdf.generation.max-pdf-size-bytes:52428800}") long maxPdfSizeBytes,
            MeterRegistry meterRegistry) {
        if (maxConcurrentRenders < 1)
            throw new IllegalStateException(
                    "app.pdf.generation.max-concurrent-renders must be >= 1, got: " + maxConcurrentRenders);
        if (maxPdfSizeBytes < 1)
            throw new IllegalStateException(
                    "app.pdf.generation.max-pdf-size-bytes must be >= 1, got: " + maxPdfSizeBytes);
        this.maxPdfSizeBytes = maxPdfSizeBytes;
        try {
            this.fopFactory       = createFopFactory();
            TransformerFactory tf = TransformerFactory.newInstance();
            this.cachedTemplates  = compileTemplates(tf, DEBITCREDITNOTE_XSL_PATH);
            this.renderSemaphore  = new Semaphore(maxConcurrentRenders, true);
            this.renderTimer      = meterRegistry.timer("pdf.fop.render");
            this.pdfSizeSummary   = DistributionSummary.builder("pdf.fop.size.bytes")
                    .description("Size of generated debit/credit note PDFs in bytes")
                    .register(meterRegistry);
            Gauge.builder("pdf.fop.render.available_permits", renderSemaphore, Semaphore::availablePermits)
                    .description("Available FOP concurrent render permits")
                    .register(meterRegistry);
            log.info("FopDebitCreditNotePdfGenerator initialized: maxConcurrentRenders={} maxPdfSizeBytes={}",
                    maxConcurrentRenders, maxPdfSizeBytes);
            checkFontAvailability();
        } catch (Exception e) {
            throw new PdfInitializationException(
                    "Failed to initialize FOP PDF generator: " + e.getMessage(), e);
        }
    }

    private Templates compileTemplates(TransformerFactory tf, String xslPath) throws Exception {
        ClassPathResource xslResource = new ClassPathResource(xslPath);
        if (!xslResource.exists())
            throw new IllegalStateException("XSL template not found at startup: " + xslPath);
        try (InputStream is = xslResource.getInputStream()) {
            return tf.newTemplates(new StreamSource(is));
        }
    }

    private FopFactory createFopFactory() throws Exception {
        URI baseUri = resolveBaseUri();
        try {
            ClassPathResource configResource = new ClassPathResource(FOP_CONFIG_PATH);
            if (configResource.exists()) {
                try (InputStream configStream = configResource.getInputStream()) {
                    return FopFactory.newInstance(baseUri, configStream);
                }
            } else {
                log.warn("FOP config not found at {}, using default configuration", FOP_CONFIG_PATH);
                return FopFactory.newInstance(baseUri);
            }
        } catch (Exception e) {
            log.warn("Failed to load FOP config, using default: {}", e.getMessage());
            return FopFactory.newInstance(baseUri);
        }
    }

    private URI resolveBaseUri() {
        try {
            URL classpathRoot = new ClassPathResource("").getURL();
            return classpathRoot.toURI();
        } catch (Exception e) {
            log.warn("Could not resolve classpath root URI for FOP: {}", e.getMessage());
            return URI.create("file:" + System.getProperty("user.dir", ".") + "/");
        }
    }

    public void checkFontAvailability() {
        List<String> missing = REQUIRED_FONTS.stream()
                .filter(font -> !new ClassPathResource(font).exists())
                .toList();
        if (!missing.isEmpty()) {
            log.warn("Thai font files not found on classpath: {} — Thai text may not render correctly.", missing);
        } else {
            log.info("Font check: all {} required Thai font files present.", REQUIRED_FONTS.size());
        }
    }

    @NewSpan("pdf.fop.render")
    public byte[] generatePdf(String xmlData, Map<String, Object> params) throws PdfGenerationException {
        log.debug("Awaiting render permit (available={})", renderSemaphore.availablePermits());
        try {
            renderSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PdfGenerationException("PDF generation interrupted while waiting for render slot", e);
        }
        long t0 = System.nanoTime();
        try {
            Transformer transformer = cachedTemplates.newTransformer();
            if (params != null) params.forEach(transformer::setParameter);
            return renderPdf(xmlData, transformer);
        } catch (javax.xml.transform.TransformerConfigurationException e) {
            throw new PdfGenerationException("Failed to create transformer: " + e.getMessage(), e);
        } finally {
            try { renderTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS); }
            finally { renderSemaphore.release(); }
        }
    }

    public byte[] generatePdf(String xmlData) throws PdfGenerationException {
        return generatePdf(xmlData, null);
    }

    private byte[] renderPdf(String xmlData, Transformer transformer) throws PdfGenerationException {
        try (ByteArrayOutputStream pdfOutput = new ByteArrayOutputStream()) {
            Fop fop = fopFactory.newFop(MimeConstants.MIME_PDF, pdfOutput);
            Source xmlSource = new StreamSource(
                new ByteArrayInputStream(xmlData.getBytes(StandardCharsets.UTF_8)));
            Result result = new SAXResult(fop.getDefaultHandler());
            transformer.transform(xmlSource, result);
            byte[] pdfBytes = pdfOutput.toByteArray();
            if (pdfBytes.length > maxPdfSizeBytes)
                throw new PdfGenerationException(
                        String.format("Generated PDF exceeds max allowed size: %d bytes > %d bytes",
                                pdfBytes.length, maxPdfSizeBytes));
            log.info("Generated PDF: {} bytes", pdfBytes.length);
            pdfSizeSummary.record(pdfBytes.length);
            return pdfBytes;
        } catch (Exception e) {
            log.error("Failed to generate PDF", e);
            throw new PdfGenerationException("PDF generation failed: " + e.getMessage(), e);
        }
    }

    public static class PdfGenerationException extends Exception {
        public PdfGenerationException(String message) { super(message); }
        public PdfGenerationException(String message, Throwable cause) { super(message, cause); }
    }

    public static class PdfInitializationException extends RuntimeException {
        public PdfInitializationException(String message) { super(message); }
        public PdfInitializationException(String message, Throwable cause) { super(message, cause); }
    }
}
