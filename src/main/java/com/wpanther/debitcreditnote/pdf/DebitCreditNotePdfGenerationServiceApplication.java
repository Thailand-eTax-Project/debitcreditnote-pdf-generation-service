package com.wpanther.debitcreditnote.pdf;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DebitCreditNotePdfGenerationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DebitCreditNotePdfGenerationServiceApplication.class, args);
    }
}
