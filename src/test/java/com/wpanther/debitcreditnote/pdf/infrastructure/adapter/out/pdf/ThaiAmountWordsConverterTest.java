package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThaiAmountWordsConverterTest {

    @Test
    void zero() {
        assertThat(ThaiAmountWordsConverter.toWords(BigDecimal.ZERO)).isEqualTo("ศูนย์บาทถ้วน");
    }

    @Test
    void oneHundred() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("100"))).isEqualTo("หนึ่งร้อยบาทถ้วน");
    }

    @Test
    void withSatang() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("1.50"))).contains("สตางค์");
    }

    @Test
    void negativeThrows() {
        assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(new BigDecimal("-1")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullThrows() {
        assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void oneMillion() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("1000000"))).contains("ล้าน");
    }

    @Test
    void twentyOne() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("21"))).contains("ยี่สิบ");
    }

    @Test
    void elevenContainsEt() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("11"))).contains("เอ็ด");
    }

    @Test
    void withExactSatang() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("100.25")))
                .isEqualTo("หนึ่งร้อยบาทยี่สิบห้าสตางค์");
    }

    @Test
    void largeNumber() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("1234567.89"));
        assertThat(result).contains("ล้าน").contains("บาท").contains("สตางค์");
    }

    @Test
    void oneBaht() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("1"))).isEqualTo("หนึ่งบาทถ้วน");
    }

    @Test
    void roundingApplied_halfUp() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("1.005")))
                .isEqualTo("หนึ่งบาทหนึ่งสตางค์");
    }
}
