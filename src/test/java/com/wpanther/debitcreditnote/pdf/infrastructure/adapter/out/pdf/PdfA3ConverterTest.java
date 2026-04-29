package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("PdfA3Converter Unit Tests")
class PdfA3ConverterTest {

    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    @Test
    @DisplayName("Constructor creates converter instance with ICC profile on classpath")
    void constructor_createsInstance() {
        assertThatNoException().isThrownBy(() -> new PdfA3Converter("icc/sRGB.icc", meterRegistry));
    }

    @Test
    @DisplayName("Constructor throws IllegalStateException for missing ICC profile")
    void constructor_throwsForMissingIccProfile() {
        assertThatThrownBy(() -> new PdfA3Converter("icc/nonexistent.icc", meterRegistry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ICC profile not found");
    }

    @Test
    @DisplayName("PdfConversionException can be created with message only")
    void testPdfConversionException_MessageOnly() {
        PdfA3Converter.PdfConversionException exception =
                new PdfA3Converter.PdfConversionException("test error");
        assertThat(exception).hasMessage("test error");
    }

    @Test
    @DisplayName("PdfConversionException can be created with message and cause")
    void testPdfConversionException_MessageAndCause() {
        Throwable cause = new RuntimeException("Root cause");
        PdfA3Converter.PdfConversionException exception =
                new PdfA3Converter.PdfConversionException("Test error", cause);

        assertThat(exception).hasMessage("Test error");
        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    @DisplayName("PdfConversionException is an Exception subtype")
    void testPdfConversionException_isException() {
        PdfA3Converter.PdfConversionException exception =
                new PdfA3Converter.PdfConversionException("msg");
        assertThat(exception).isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("convertToPdfA3 throws PdfConversionException for null input")
    void testConvertToPdfA3_NullInput_Throws() {
        PdfA3Converter converter = new PdfA3Converter("icc/sRGB.icc", meterRegistry);
        assertThatThrownBy(() -> converter.convertToPdfA3(null, "<xml/>", "test.xml", "DCN-001"))
                .isInstanceOf(PdfA3Converter.PdfConversionException.class);
    }

    @Test
    @DisplayName("convertToPdfA3 throws PdfConversionException for invalid PDF bytes")
    void testConvertToPdfA3_InvalidPdfBytes_Throws() {
        PdfA3Converter converter = new PdfA3Converter("icc/sRGB.icc", meterRegistry);
        byte[] invalidBytes = "not a valid pdf".getBytes();
        assertThatThrownBy(() -> converter.convertToPdfA3(invalidBytes, "<xml/>", "test.xml", "DCN-001"))
                .isInstanceOf(PdfA3Converter.PdfConversionException.class);
    }
}
