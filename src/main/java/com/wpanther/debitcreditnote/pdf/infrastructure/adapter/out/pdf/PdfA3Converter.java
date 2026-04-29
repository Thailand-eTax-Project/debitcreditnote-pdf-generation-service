package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDDocumentNameDictionary;
import org.apache.pdfbox.pdmodel.PDEmbeddedFilesNameTreeNode;
import org.apache.pdfbox.pdmodel.common.PDMetadata;
import org.apache.pdfbox.pdmodel.common.filespecification.PDComplexFileSpecification;
import org.apache.pdfbox.pdmodel.common.filespecification.PDEmbeddedFile;
import org.apache.pdfbox.pdmodel.graphics.color.PDOutputIntent;
import org.apache.xmpbox.XMPMetadata;
import org.apache.xmpbox.schema.DublinCoreSchema;
import org.apache.xmpbox.schema.PDFAIdentificationSchema;
import org.apache.xmpbox.schema.XMPBasicSchema;
import org.apache.xmpbox.xml.XmpSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class PdfA3Converter {

    private static final String ICC_PROFILE_PATH = "icc/sRGB.icc";
    private static final String MIME_TYPE_XML = "application/xml";
    private static final String AFRelationship_SOURCE = "Source";

    private final String iccProfilePath;
    private final Timer conversionTimer;

    public PdfA3Converter(@Value("${app.pdf.icc-profile-path:icc/sRGB.icc}") String iccProfilePath,
                          MeterRegistry meterRegistry) {
        this.iccProfilePath = iccProfilePath;
        this.conversionTimer = meterRegistry.timer("pdf.conversion.pdfa3");
        loadIccProfile();
    }

    public byte[] convertToPdfA3(byte[] pdfBytes, String xmlContent, String xmlFilename, String taxInvoiceNumber)
            throws PdfConversionException {

        log.debug("Converting PDF to PDF/A-3 with embedded XML: {}", xmlFilename);

        long t0 = System.nanoTime();
        try (PDDocument document = Loader.loadPDF(pdfBytes)) {

            addPdfAMetadata(document, taxInvoiceNumber);
            addColorProfile(document);
            embedXmlFile(document, xmlContent, xmlFilename);

            try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                document.save(output);
                byte[] result = output.toByteArray();
                log.info("Converted to PDF/A-3: {} bytes (XML embedded: {})", result.length, xmlFilename);
                return result;
            }

        } catch (Exception e) {
            log.error("Failed to convert PDF to PDF/A-3", e);
            throw new PdfConversionException("PDF/A-3 conversion failed: " + e.getMessage(), e);
        } finally {
            conversionTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
        }
    }

    private void addPdfAMetadata(PDDocument document, String taxInvoiceNumber) throws Exception {
        XMPMetadata xmp = XMPMetadata.createXMPMetadata();

        PDFAIdentificationSchema pdfaId = xmp.createAndAddPDFAIdentificationSchema();
        pdfaId.setPart(3);
        pdfaId.setConformance("B");

        DublinCoreSchema dc = xmp.createAndAddDublinCoreSchema();
        dc.setTitle("Thai e-Tax Debit/Credit Note: " + taxInvoiceNumber);
        dc.setDescription("Electronic debit/credit note with embedded XML source");
        dc.addCreator("Debit/Credit Note PDF Generation Service");
        dc.setFormat("application/pdf");

        XMPBasicSchema xmpBasic = xmp.createAndAddXMPBasicSchema();
        Calendar now = GregorianCalendar.from(LocalDateTime.now().atZone(ZoneId.systemDefault()));
        xmpBasic.setCreateDate(now);
        xmpBasic.setModifyDate(now);
        xmpBasic.setCreatorTool("Thai e-Tax Debit/Credit Note System");

        XmpSerializer serializer = new XmpSerializer();
        ByteArrayOutputStream xmpOutput = new ByteArrayOutputStream();
        serializer.serialize(xmp, xmpOutput, true);

        PDMetadata metadata = new PDMetadata(document);
        metadata.importXMPMetadata(xmpOutput.toByteArray());

        PDDocumentCatalog catalog = document.getDocumentCatalog();
        catalog.setMetadata(metadata);
    }

    private void addColorProfile(PDDocument document) throws Exception {
        PDDocumentCatalog catalog = document.getDocumentCatalog();

        if (catalog.getOutputIntents() != null && !catalog.getOutputIntents().isEmpty()) {
            log.debug("Output intent already exists, skipping ICC profile");
            return;
        }

        try (InputStream iccStream = loadIccProfile()) {
            PDOutputIntent outputIntent = new PDOutputIntent(document, iccStream);
            outputIntent.setInfo("sRGB IEC61966-2.1");
            outputIntent.setOutputCondition("sRGB");
            outputIntent.setOutputConditionIdentifier("sRGB IEC61966-2.1");
            outputIntent.setRegistryName("http://www.color.org");
            catalog.addOutputIntent(outputIntent);
            log.debug("Added sRGB ICC color profile");
        }
    }

    private InputStream loadIccProfile() {
        ClassPathResource iccResource = new ClassPathResource(iccProfilePath);
        if (iccResource.exists()) {
            try {
                InputStream is = iccResource.getInputStream();
                log.info("Loaded ICC profile: {} (cached for reuse)", iccProfilePath);
                return is;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to read ICC profile from " + iccProfilePath + ": " + e.getMessage(), e);
            }
        }
        throw new IllegalStateException(
                "ICC profile not found on classpath: " + iccProfilePath
                + " — add sRGB.icc to src/main/resources/icc/ (PDF/A-3 compliance requires it)");
    }

    private void embedXmlFile(PDDocument document, String xmlContent, String xmlFilename) throws Exception {
        PDDocumentCatalog catalog = document.getDocumentCatalog();

        byte[] xmlBytes = xmlContent.getBytes(StandardCharsets.UTF_8);
        PDEmbeddedFile embeddedFile = new PDEmbeddedFile(document, new ByteArrayInputStream(xmlBytes));
        embeddedFile.setSubtype(MIME_TYPE_XML);
        embeddedFile.setSize(xmlBytes.length);
        embeddedFile.setCreationDate(new GregorianCalendar());
        embeddedFile.setModDate(new GregorianCalendar());

        PDComplexFileSpecification fileSpec = new PDComplexFileSpecification();
        fileSpec.setFile(xmlFilename);
        fileSpec.setFileUnicode(xmlFilename);
        fileSpec.setEmbeddedFile(embeddedFile);
        fileSpec.setEmbeddedFileUnicode(embeddedFile);
        fileSpec.getCOSObject().setName(COSName.getPDFName("AFRelationship"), AFRelationship_SOURCE);

        PDEmbeddedFilesNameTreeNode embeddedFilesTree = new PDEmbeddedFilesNameTreeNode();
        embeddedFilesTree.setNames(Collections.singletonMap(xmlFilename, fileSpec));

        PDDocumentNameDictionary nameDictionary = catalog.getNames();
        if (nameDictionary == null) {
            nameDictionary = new PDDocumentNameDictionary(catalog);
            catalog.setNames(nameDictionary);
        }
        nameDictionary.setEmbeddedFiles(embeddedFilesTree);
        catalog.getCOSObject().setItem(COSName.getPDFName("AF"), fileSpec);

        log.debug("Embedded XML file: {} ({} bytes)", xmlFilename, xmlBytes.length);
    }

    public static class PdfConversionException extends Exception {
        public PdfConversionException(String message) { super(message); }
        public PdfConversionException(String message, Throwable cause) { super(message, cause); }
    }
}
