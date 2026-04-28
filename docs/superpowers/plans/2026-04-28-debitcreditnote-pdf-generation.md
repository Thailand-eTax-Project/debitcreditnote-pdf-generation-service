# Debit/Credit Note PDF Generation Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `debitcreditnote-pdf-generation-service` — a Spring Boot microservice that generates PDF/A-3 documents for Thai e-Tax Debit/Credit Notes and participates in the Saga Orchestration pipeline via MinIO + transactional outbox.

**Architecture:** Full hexagonal (port/adapter) port of `taxinvoice-pdf-generation-service` (port 8089). Domain model, application services, Kafka/Camel adapters, MinIO storage, and FOP-based XSL-FO PDF pipeline are structurally identical; only DebitCreditNote-specific namespaces, class names, and the XSL template differ. Both Debit Notes (TypeCode=80) and Credit Notes (TypeCode=81) share the same XML schema and are handled by this single service.

**Tech Stack:** Java 21, Spring Boot 3.2.5, Apache Camel 4.14.4, Apache FOP 2.9, Apache PDFBox 3.0.1, MinIO (AWS SDK v2 S3), PostgreSQL + Flyway, Resilience4j, Micrometer/OTLP, JUnit 5 + AssertJ, H2 (tests).

**Reference service:** `invoice-microservices/services/taxinvoice-pdf-generation-service/` — mirror every file, substituting all `TaxInvoice`/`taxinvoice`/`tax-invoice` identifiers with `DebitCreditNote`/`debitcreditnote`/`debit-credit-note` equivalents, and updating namespace URIs and XSL template as specified below.

**Namespace substitution table (used throughout):**

| TaxInvoice value | DebitCreditNote value |
|------------------|-----------------------|
| `urn:etda:uncefact:data:standard:TaxInvoice_CrossIndustryInvoice:2` | `urn:etda:uncefact:data:standard:DebitCreditNote_CrossIndustryInvoice:2` |
| `urn:etda:uncefact:data:standard:TaxInvoice_ReusableAggregateBusinessInformationEntity:2` | `urn:etda:uncefact:data:standard:DebitCreditNote_ReusableAggregateBusinessInformationEntity:2` |
| `/rsm:TaxInvoice_CrossIndustryInvoice` (XSL root match) | `/rsm:DebitCreditNote_CrossIndustryInvoice` |

**All work is done inside:** `invoice-microservices/services/debitcreditnote-pdf-generation-service/`  
**Saga commons is at:** `invoice-microservices/../saga-commons/`  (i.e. `etax/saga-commons/`)

---

## File Map

### saga-commons (modify)
- `saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java` — add `GENERATE_DEBIT_CREDIT_NOTE_PDF`

### debitcreditnote-pdf-generation-service (create all)

**Build**
- `pom.xml`
- `src/main/java/com/wpanther/debitcreditnote/pdf/DebitCreditNotePdfGenerationServiceApplication.java`

**Domain**
- `src/main/java/com/wpanther/debitcreditnote/pdf/domain/model/GenerationStatus.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/domain/model/DebitCreditNotePdfDocument.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/domain/exception/DebitCreditNotePdfGenerationException.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/domain/constants/PdfGenerationConstants.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/domain/repository/DebitCreditNotePdfDocumentRepository.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/domain/service/DebitCreditNotePdfGenerationService.java`

**Application**
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/PdfEventPort.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/PdfStoragePort.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/SagaReplyPort.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/SignedXmlFetchPort.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/ProcessDebitCreditNotePdfUseCase.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/CompensateDebitCreditNotePdfUseCase.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/service/DebitCreditNotePdfDocumentService.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandler.java`

**Infrastructure — Kafka (in)**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteProcessCommand.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteCompensateCommand.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java`

**Infrastructure — messaging (out)**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/OutboxConstants.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/DebitCreditNotePdfGeneratedEvent.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/EventPublisher.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java`

**Infrastructure — PDF (out)**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverter.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/FopDebitCreditNotePdfGenerator.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/PdfA3Converter.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/DebitCreditNotePdfGenerationServiceImpl.java`

**Infrastructure — persistence (out)**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/outbox/OutboxEventEntity.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/outbox/SpringDataOutboxRepository.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/outbox/JpaOutboxEventRepository.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/DebitCreditNotePdfDocumentEntity.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/JpaDebitCreditNotePdfDocumentRepository.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/DebitCreditNotePdfDocumentRepositoryAdapter.java`

**Infrastructure — storage (out)**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/storage/MinioStorageAdapter.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/storage/MinioCleanupService.java`

**Infrastructure — REST client (out)**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcher.java`

**Infrastructure — config**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/MinioConfig.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/OutboxConfig.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/RestTemplateConfig.java`
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/FontHealthCheck.java`

**Infrastructure — metrics**
- `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/metrics/PdfGenerationMetrics.java`

**Resources**
- `src/main/resources/application.yml`
- `src/main/resources/db/migration/V1__create_debit_credit_note_pdf_tables.sql`
- `src/main/resources/xsl/debitcreditnote-direct.xsl`
- `src/main/resources/fop/fop.xconf` (copy from taxinvoice)
- `src/main/resources/icc/sRGB.icc` (copy from taxinvoice)
- `src/main/resources/fonts/` (copy all 6 font files from taxinvoice)

**Tests**
- `src/test/resources/application-test.yml`
- `src/test/resources/fop/fop.xconf`
- `src/test/resources/xml/preview-debitcreditnote.xml`
- `src/test/java/com/wpanther/debitcreditnote/pdf/domain/model/DebitCreditNotePdfDocumentTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/domain/exception/DebitCreditNotePdfGenerationExceptionTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/domain/constants/PdfGenerationConstantsTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandlerTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/application/service/DebitCreditNotePdfDocumentServiceTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/EventPublisherTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisherTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/FopDebitCreditNotePdfGeneratorTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/PdfA3ConverterTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverterTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/DebitCreditNotePdfGenerationServiceImplTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/JpaDebitCreditNotePdfDocumentRepositoryImplTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/storage/MinioStorageAdapterTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/storage/MinioCleanupServiceTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcherTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/CamelRouteConfigTest.java`
- `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/FontHealthCheckTest.java`

---

## Task 1: Saga Commons — Add `GENERATE_DEBIT_CREDIT_NOTE_PDF` enum value

**Files:**
- Modify: `etax/saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java`

- [ ] **Step 1: Open `SagaStep.java` and locate the insertion point**

The file is at `etax/saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java`.  
Find the line containing `GENERATE_ABBREVIATED_TAX_INVOICE_PDF` (currently the last PDF generation step before `SIGN_PDF`).

- [ ] **Step 2: Add the new enum value after `GENERATE_ABBREVIATED_TAX_INVOICE_PDF`**

Insert after:
```java
    GENERATE_ABBREVIATED_TAX_INVOICE_PDF("generate-abbreviated-tax-invoice-pdf", "Abbreviated Tax Invoice PDF Generation Service"),
```

Add:
```java
    /**
     * Debit/Credit note PDF generation via debitcreditnote-pdf-generation-service.
     */
    GENERATE_DEBIT_CREDIT_NOTE_PDF("generate-debit-credit-note-pdf", "Debit/Credit Note PDF Generation Service"),
```

- [ ] **Step 3: Build saga-commons to verify no compilation errors**

```bash
cd etax/saga-commons && mvn clean install -q
```

Expected: `BUILD SUCCESS`

- [ ] **Step 4: Commit**

```bash
cd etax/saga-commons
git add src/main/java/com/wpanther/saga/domain/enums/SagaStep.java
git commit -m "feat: add GENERATE_DEBIT_CREDIT_NOTE_PDF saga step"
```

---

## Task 2: Maven project scaffold — `pom.xml` + main application class

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/DebitCreditNotePdfGenerationServiceApplication.java`

- [ ] **Step 1: Create `pom.xml`**

Copy `taxinvoice-pdf-generation-service/pom.xml` as a base, then apply these substitutions:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.wpanther</groupId>
    <artifactId>debitcreditnote-pdf-generation-service</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>Debit/Credit Note PDF Generation Service</name>
    <description>Microservice for generating PDF/A-3 documents for Thai e-Tax Debit/Credit Notes with embedded XML</description>

    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <spring-boot.version>3.2.5</spring-boot.version>
        <spring-cloud.version>2023.0.1</spring-cloud.version>
        <fop.version>2.9</fop.version>
        <pdfbox.version>3.0.1</pdfbox.version>
        <lombok.version>1.18.30</lombok.version>
        <flyway.version>10.10.0</flyway.version>
        <camel.version>4.14.4</camel.version>
        <saga.commons.version>1.0.0-SNAPSHOT</saga.commons.version>
        <aws-sdk.version>2.20.26</aws-sdk.version>
        <resilience4j.version>2.1.0</resilience4j.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.springframework.cloud</groupId>
                <artifactId>spring-cloud-dependencies</artifactId>
                <version>${spring-cloud.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.camel.springboot</groupId>
                <artifactId>camel-spring-boot-bom</artifactId>
                <version>${camel.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>software.amazon.awssdk</groupId>
                <artifactId>bom</artifactId>
                <version>${aws-sdk.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.camel.springboot</groupId>
            <artifactId>camel-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.camel.springboot</groupId>
            <artifactId>camel-kafka-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.camel.springboot</groupId>
            <artifactId>camel-jackson-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-netflix-eureka-client</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
            <version>${flyway.version}</version>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
            <version>${flyway.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.xmlgraphics</groupId>
            <artifactId>fop</artifactId>
            <version>${fop.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.pdfbox</groupId>
            <artifactId>pdfbox</artifactId>
            <version>${pdfbox.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.pdfbox</groupId>
            <artifactId>xmpbox</artifactId>
            <version>${pdfbox.version}</version>
        </dependency>
        <dependency>
            <groupId>jakarta.xml.bind</groupId>
            <artifactId>jakarta.xml.bind-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.glassfish.jaxb</groupId>
            <artifactId>jaxb-runtime</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.datatype</groupId>
            <artifactId>jackson-datatype-jsr310</artifactId>
        </dependency>
        <dependency>
            <groupId>com.wpanther</groupId>
            <artifactId>saga-commons</artifactId>
            <version>${saga.commons.version}</version>
        </dependency>
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>s3</artifactId>
        </dependency>
        <dependency>
            <groupId>io.github.resilience4j</groupId>
            <artifactId>resilience4j-spring-boot3</artifactId>
            <version>${resilience4j.version}</version>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-tracing-bridge-otel</artifactId>
        </dependency>
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-exporter-otlp</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.camel</groupId>
            <artifactId>camel-test-spring-junit5</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>repackage</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
                <configuration>
                    <source>21</source>
                    <target>21</target>
                    <annotationProcessorPaths>
                        <path>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                            <version>${lombok.version}</version>
                        </path>
                    </annotationProcessorPaths>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.flywaydb</groupId>
                <artifactId>flyway-maven-plugin</artifactId>
                <version>${flyway.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Create main application class**

Create `src/main/java/com/wpanther/debitcreditnote/pdf/DebitCreditNotePdfGenerationServiceApplication.java`:

```java
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
```

- [ ] **Step 3: Verify the project compiles (domain classes don't exist yet, so use `-pl` with no tests)**

```bash
cd invoice-microservices/services/debitcreditnote-pdf-generation-service
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD FAILURE` with "package does not exist" for missing domain classes — this confirms Maven resolves saga-commons correctly. If you see "Cannot find teda" or "Cannot find saga-commons", run `cd etax/teda && mvn install -q` and `cd etax/saga-commons && mvn install -q` first.

- [ ] **Step 4: Commit**

```bash
git add pom.xml src/main/java/com/wpanther/debitcreditnote/pdf/DebitCreditNotePdfGenerationServiceApplication.java
git commit -m "feat: scaffold debitcreditnote-pdf-generation-service project"
```

---

## Task 3: Domain — exception, constants, `GenerationStatus`, repository interface, domain service port

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/domain/exception/DebitCreditNotePdfGenerationException.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/domain/constants/PdfGenerationConstants.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/domain/model/GenerationStatus.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/domain/repository/DebitCreditNotePdfDocumentRepository.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/domain/service/DebitCreditNotePdfGenerationService.java`
- Test: `src/test/java/com/wpanther/debitcreditnote/pdf/domain/exception/DebitCreditNotePdfGenerationExceptionTest.java`
- Test: `src/test/java/com/wpanther/debitcreditnote/pdf/domain/constants/PdfGenerationConstantsTest.java`

- [ ] **Step 1: Create `DebitCreditNotePdfGenerationException.java`**

```java
package com.wpanther.debitcreditnote.pdf.domain.exception;

public class DebitCreditNotePdfGenerationException extends RuntimeException {

    public DebitCreditNotePdfGenerationException(String message) {
        super(message);
    }

    public DebitCreditNotePdfGenerationException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 2: Create `PdfGenerationConstants.java`**

```java
package com.wpanther.debitcreditnote.pdf.domain.constants;

public final class PdfGenerationConstants {

    private PdfGenerationConstants() {}

    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final String PDF_MIME_TYPE = "application/pdf";
}
```

- [ ] **Step 3: Create `GenerationStatus.java`**

```java
package com.wpanther.debitcreditnote.pdf.domain.model;

public enum GenerationStatus {
    PENDING,
    GENERATING,
    COMPLETED,
    FAILED
}
```

- [ ] **Step 4: Create `DebitCreditNotePdfDocumentRepository.java`**

```java
package com.wpanther.debitcreditnote.pdf.domain.repository;

import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import java.util.Optional;
import java.util.UUID;

public interface DebitCreditNotePdfDocumentRepository {

    DebitCreditNotePdfDocument save(DebitCreditNotePdfDocument document);

    Optional<DebitCreditNotePdfDocument> findById(UUID id);

    Optional<DebitCreditNotePdfDocument> findByDebitCreditNoteId(String debitCreditNoteId);

    void deleteById(UUID id);

    void flush();
}
```

- [ ] **Step 5: Create `DebitCreditNotePdfGenerationService.java` (domain service port)**

```java
package com.wpanther.debitcreditnote.pdf.domain.service;

public interface DebitCreditNotePdfGenerationService {

    byte[] generatePdf(String signedXml);
}
```

- [ ] **Step 6: Write tests for exception and constants**

Create `src/test/java/com/wpanther/debitcreditnote/pdf/domain/exception/DebitCreditNotePdfGenerationExceptionTest.java`:

```java
package com.wpanther.debitcreditnote.pdf.domain.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DebitCreditNotePdfGenerationExceptionTest {

    @Test
    void messageConstructor() {
        var ex = new DebitCreditNotePdfGenerationException("error");
        assertThat(ex.getMessage()).isEqualTo("error");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void messageCauseConstructor() {
        var cause = new RuntimeException("root");
        var ex = new DebitCreditNotePdfGenerationException("error", cause);
        assertThat(ex.getMessage()).isEqualTo("error");
        assertThat(ex.getCause()).isSameAs(cause);
    }
}
```

Create `src/test/java/com/wpanther/debitcreditnote/pdf/domain/constants/PdfGenerationConstantsTest.java`:

```java
package com.wpanther.debitcreditnote.pdf.domain.constants;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdfGenerationConstantsTest {

    @Test
    void defaultMaxRetries() {
        assertThat(PdfGenerationConstants.DEFAULT_MAX_RETRIES).isEqualTo(3);
    }

    @Test
    void pdfMimeType() {
        assertThat(PdfGenerationConstants.PDF_MIME_TYPE).isEqualTo("application/pdf");
    }
}
```

- [ ] **Step 7: Create minimal `application-test.yml` so tests can run (H2, no Flyway, no Kafka)**

Create `src/test/resources/application-test.yml`:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    username: sa
    password:
    driver-class-name: org.h2.Driver
  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop
    show-sql: false
  flyway:
    enabled: false

app:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      command-group-id: test-command
      compensation-group-id: test-compensation
      break-on-first-error: false
      max-poll-records: 10
      consumers-count: 1
    topics:
      saga-command-debit-credit-note-pdf: saga.command.debit-credit-note-pdf
      saga-compensation-debit-credit-note-pdf: saga.compensation.debit-credit-note-pdf
      pdf-generated-debit-credit-note: pdf.generated.debit-credit-note
      dlq: pdf.generation.debit-credit-note.dlq
  minio:
    endpoint: http://localhost:9000
    access-key: minioadmin
    secret-key: minioadmin
    bucket-name: debitcreditnotes
    region: us-east-1
    base-url: http://localhost:9000/debitcreditnotes
    path-style-access: true
    cleanup:
      enabled: false
      cron: "0 0 2 * * ?"
  pdf:
    icc-profile-path: icc/sRGB.icc
    generation:
      max-retries: 3
      max-concurrent-renders: 3
      max-pdf-size-bytes: 52428800
  debitcreditnote:
    default-vat-rate: 7
  rest-client:
    connect-timeout: 5000
    read-timeout: 10000
    allowed-hosts: localhost
  fonts:
    health-check:
      enabled: false
      fail-on-error: false

eureka:
  client:
    enabled: false

management:
  tracing:
    enabled: false
```

- [ ] **Step 8: Run the two new tests to verify they pass**

```bash
cd invoice-microservices/services/debitcreditnote-pdf-generation-service
mvn test -Dtest="DebitCreditNotePdfGenerationExceptionTest,PdfGenerationConstantsTest" -Dspring.profiles.active=test 2>&1 | tail -10
```

Expected: `Tests run: 4, Failures: 0, Errors: 0`

- [ ] **Step 9: Commit**

```bash
git add src/
git commit -m "feat: add domain exception, constants, GenerationStatus, repository interface, service port"
```

---

## Task 4: Domain model — `DebitCreditNotePdfDocument` aggregate with tests

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/domain/model/DebitCreditNotePdfDocument.java`
- Test: `src/test/java/com/wpanther/debitcreditnote/pdf/domain/model/DebitCreditNotePdfDocumentTest.java`

- [ ] **Step 1: Write the failing test first**

Create `src/test/java/com/wpanther/debitcreditnote/pdf/domain/model/DebitCreditNotePdfDocumentTest.java`:

```java
package com.wpanther.debitcreditnote.pdf.domain.model;

import com.wpanther.debitcreditnote.pdf.domain.exception.DebitCreditNotePdfGenerationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@DisplayName("DebitCreditNotePdfDocument Aggregate Tests")
class DebitCreditNotePdfDocumentTest {

    private DebitCreditNotePdfDocument pendingDocument() {
        return DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-2024-001")
                .build();
    }

    @Test
    @DisplayName("Should create document in PENDING status with defaults")
    void testCreate_Defaults() {
        DebitCreditNotePdfDocument doc = pendingDocument();

        assertThat(doc.getId()).isNotNull();
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.PENDING);
        assertThat(doc.getMimeType()).isEqualTo("application/pdf");
        assertThat(doc.getRetryCount()).isZero();
        assertThat(doc.isXmlEmbedded()).isFalse();
        assertThat(doc.getCreatedAt()).isNotNull();
    }

    @Test
    @DisplayName("Should reject blank debitCreditNoteId")
    void testCreate_BlankDebitCreditNoteId() {
        assertThatThrownBy(() ->
                DebitCreditNotePdfDocument.builder()
                        .debitCreditNoteId("   ")
                        .documentNumber("DCN-001")
                        .build()
        ).isInstanceOf(DebitCreditNotePdfGenerationException.class)
         .hasMessageContaining("Debit/Credit Note ID cannot be blank");
    }

    @Test
    @DisplayName("Should reject null documentNumber")
    void testCreate_NullDocumentNumber() {
        assertThatThrownBy(() ->
                DebitCreditNotePdfDocument.builder()
                        .debitCreditNoteId("dcn-001")
                        .documentNumber(null)
                        .build()
        ).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("PENDING → startGeneration() → GENERATING")
    void testStartGeneration() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.GENERATING);
    }

    @Test
    @DisplayName("GENERATING → markCompleted() → COMPLETED")
    void testMarkCompleted() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();
        doc.markCompleted("2024/01/15/test.pdf", "http://minio/test.pdf", 12345L);

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
        assertThat(doc.getDocumentPath()).isEqualTo("2024/01/15/test.pdf");
        assertThat(doc.getDocumentUrl()).isEqualTo("http://minio/test.pdf");
        assertThat(doc.getFileSize()).isEqualTo(12345L);
        assertThat(doc.getCompletedAt()).isNotNull();
        assertThat(doc.isCompleted()).isTrue();
        assertThat(doc.isSuccessful()).isTrue();
    }

    @Test
    @DisplayName("Any state → markFailed() → FAILED")
    void testMarkFailed_FromPending() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.markFailed("Something went wrong");

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.FAILED);
        assertThat(doc.getErrorMessage()).isEqualTo("Something went wrong");
        assertThat(doc.isFailed()).isTrue();
        assertThat(doc.isCompleted()).isFalse();
        assertThat(doc.getCompletedAt()).isNotNull();
    }

    @Test
    @DisplayName("PENDING → markXmlEmbedded() sets flag")
    void testMarkXmlEmbedded() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        assertThat(doc.isXmlEmbedded()).isFalse();
        doc.markXmlEmbedded();
        assertThat(doc.isXmlEmbedded()).isTrue();
    }

    @Test
    @DisplayName("startGeneration() from GENERATING throws exception")
    void testStartGeneration_AlreadyGenerating() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(doc::startGeneration)
                .isInstanceOf(DebitCreditNotePdfGenerationException.class)
                .hasMessageContaining("PENDING");
    }

    @Test
    @DisplayName("markCompleted() from PENDING throws exception")
    void testMarkCompleted_FromPending() {
        DebitCreditNotePdfDocument doc = pendingDocument();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 100L))
                .isInstanceOf(DebitCreditNotePdfGenerationException.class)
                .hasMessageContaining("GENERATING");
    }

    @Test
    @DisplayName("markCompleted() with zero fileSize throws IllegalArgumentException")
    void testMarkCompleted_ZeroFileSize() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("File size must be positive");
    }

    @Test
    @DisplayName("markCompleted() with null documentPath throws NullPointerException")
    void testMarkCompleted_NullPath() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted(null, "url", 100L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("incrementRetryCount() increases count by one")
    void testIncrementRetryCount() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        assertThat(doc.getRetryCount()).isZero();
        doc.incrementRetryCount();
        assertThat(doc.getRetryCount()).isOne();
        doc.incrementRetryCount();
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("incrementRetryCountTo() advances count to target")
    void testIncrementRetryCountTo_AdvancesToTarget() {
        DebitCreditNotePdfDocument doc = pendingDocument();
        doc.incrementRetryCountTo(2);
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("incrementRetryCountTo() is a no-op when count already higher")
    void testIncrementRetryCountTo_NoOpWhenAlreadyHigher() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .retryCount(2)
                .build();
        doc.incrementRetryCountTo(1);
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("isMaxRetriesExceeded() returns true when retryCount >= maxRetries")
    void testIsMaxRetriesExceeded_AtLimit() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .retryCount(3)
                .build();
        assertThat(doc.isMaxRetriesExceeded(3)).isTrue();
    }

    @Test
    @DisplayName("isMaxRetriesExceeded() returns false when retryCount < maxRetries")
    void testIsMaxRetriesExceeded_BelowLimit() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-001")
                .retryCount(2)
                .build();
        assertThat(doc.isMaxRetriesExceeded(3)).isFalse();
    }
}
```

- [ ] **Step 2: Run the test to confirm it fails with "cannot find symbol"**

```bash
mvn test -Dtest=DebitCreditNotePdfDocumentTest -Dspring.profiles.active=test 2>&1 | tail -5
```

Expected: `COMPILATION ERROR` — `DebitCreditNotePdfDocument` does not exist yet.

- [ ] **Step 3: Create `DebitCreditNotePdfDocument.java`**

```java
package com.wpanther.debitcreditnote.pdf.domain.model;

import com.wpanther.debitcreditnote.pdf.domain.exception.DebitCreditNotePdfGenerationException;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class DebitCreditNotePdfDocument {

    private static final String DEFAULT_MIME_TYPE = "application/pdf";

    private final UUID id;
    private final String debitCreditNoteId;
    private final String documentNumber;
    private String documentPath;
    private String documentUrl;
    private long fileSize;
    private final String mimeType;
    private boolean xmlEmbedded;
    private GenerationStatus status;
    private String errorMessage;
    private int retryCount;
    private final LocalDateTime createdAt;
    private LocalDateTime completedAt;

    private DebitCreditNotePdfDocument(Builder builder) {
        this.id = builder.id != null ? builder.id : UUID.randomUUID();
        this.debitCreditNoteId = Objects.requireNonNull(builder.debitCreditNoteId, "Debit/Credit Note ID is required");
        this.documentNumber = Objects.requireNonNull(builder.documentNumber, "Document number is required");
        this.documentPath = builder.documentPath;
        this.documentUrl = builder.documentUrl;
        this.fileSize = builder.fileSize;
        this.mimeType = builder.mimeType != null ? builder.mimeType : DEFAULT_MIME_TYPE;
        this.xmlEmbedded = builder.xmlEmbedded;
        this.status = builder.status != null ? builder.status : GenerationStatus.PENDING;
        this.errorMessage = builder.errorMessage;
        this.retryCount = builder.retryCount;
        this.createdAt = builder.createdAt != null ? builder.createdAt : LocalDateTime.now();
        this.completedAt = builder.completedAt;
        validateInvariant();
    }

    private void validateInvariant() {
        if (debitCreditNoteId.isBlank()) {
            throw new DebitCreditNotePdfGenerationException("Debit/Credit Note ID cannot be blank");
        }
        if (documentNumber.isBlank()) {
            throw new DebitCreditNotePdfGenerationException("Document number cannot be blank");
        }
    }

    public void startGeneration() {
        if (this.status != GenerationStatus.PENDING) {
            throw new DebitCreditNotePdfGenerationException("Can only start generation from PENDING status");
        }
        this.status = GenerationStatus.GENERATING;
    }

    public void markCompleted(String documentPath, String documentUrl, long fileSize) {
        if (this.status != GenerationStatus.GENERATING) {
            throw new DebitCreditNotePdfGenerationException("Can only complete from GENERATING status");
        }
        Objects.requireNonNull(documentPath, "Document path is required");
        Objects.requireNonNull(documentUrl, "Document URL is required");
        if (fileSize <= 0) {
            throw new IllegalArgumentException("File size must be positive");
        }
        this.documentPath = documentPath;
        this.documentUrl = documentUrl;
        this.fileSize = fileSize;
        this.status = GenerationStatus.COMPLETED;
        this.completedAt = LocalDateTime.now();
    }

    public void markFailed(String errorMessage) {
        this.status = GenerationStatus.FAILED;
        this.errorMessage = errorMessage;
        this.completedAt = LocalDateTime.now();
    }

    public void markXmlEmbedded() {
        this.xmlEmbedded = true;
    }

    public boolean isSuccessful() { return status == GenerationStatus.COMPLETED; }
    public boolean isCompleted()  { return status == GenerationStatus.COMPLETED; }
    public boolean isFailed()     { return status == GenerationStatus.FAILED; }

    public void incrementRetryCount() { this.retryCount++; }

    public void incrementRetryCountTo(int target) {
        if (target < 0) throw new IllegalArgumentException("Target retry count cannot be negative");
        if (this.retryCount < target) this.retryCount = target;
    }

    public void setRetryCount(int retryCount) {
        if (retryCount < 0) throw new IllegalArgumentException("Retry count cannot be negative");
        this.retryCount = retryCount;
    }

    public boolean isMaxRetriesExceeded(int maxRetries) { return this.retryCount >= maxRetries; }

    public UUID getId()                  { return id; }
    public String getDebitCreditNoteId() { return debitCreditNoteId; }
    public String getDocumentNumber()    { return documentNumber; }
    public String getDocumentPath()      { return documentPath; }
    public String getDocumentUrl()       { return documentUrl; }
    public long getFileSize()            { return fileSize; }
    public String getMimeType()          { return mimeType; }
    public boolean isXmlEmbedded()       { return xmlEmbedded; }
    public GenerationStatus getStatus()  { return status; }
    public String getErrorMessage()      { return errorMessage; }
    public int getRetryCount()           { return retryCount; }
    public LocalDateTime getCreatedAt()  { return createdAt; }
    public LocalDateTime getCompletedAt(){ return completedAt; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private UUID id;
        private String debitCreditNoteId;
        private String documentNumber;
        private String documentPath;
        private String documentUrl;
        private long fileSize;
        private String mimeType;
        private boolean xmlEmbedded;
        private GenerationStatus status;
        private String errorMessage;
        private int retryCount;
        private LocalDateTime createdAt;
        private LocalDateTime completedAt;

        public Builder id(UUID id)                              { this.id = id; return this; }
        public Builder debitCreditNoteId(String v)             { this.debitCreditNoteId = v; return this; }
        public Builder documentNumber(String v)                { this.documentNumber = v; return this; }
        public Builder documentPath(String v)                  { this.documentPath = v; return this; }
        public Builder documentUrl(String v)                   { this.documentUrl = v; return this; }
        public Builder fileSize(long v)                        { this.fileSize = v; return this; }
        public Builder mimeType(String v)                      { this.mimeType = v; return this; }
        public Builder xmlEmbedded(boolean v)                  { this.xmlEmbedded = v; return this; }
        public Builder status(GenerationStatus v)              { this.status = v; return this; }
        public Builder errorMessage(String v)                  { this.errorMessage = v; return this; }
        public Builder retryCount(int v)                       { this.retryCount = v; return this; }
        public Builder createdAt(LocalDateTime v)              { this.createdAt = v; return this; }
        public Builder completedAt(LocalDateTime v)            { this.completedAt = v; return this; }

        public DebitCreditNotePdfDocument build() { return new DebitCreditNotePdfDocument(this); }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DebitCreditNotePdfDocument other)) return false;
        return Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() { return Objects.hashCode(id); }

    @Override
    public String toString() {
        return "DebitCreditNotePdfDocument{id=" + id +
               ", debitCreditNoteId='" + debitCreditNoteId + '\'' +
               ", documentNumber='" + documentNumber + '\'' +
               ", status=" + status +
               ", retryCount=" + retryCount + '}';
    }
}
```

- [ ] **Step 4: Run the domain model tests**

```bash
mvn test -Dtest=DebitCreditNotePdfDocumentTest -Dspring.profiles.active=test 2>&1 | tail -10
```

Expected: `Tests run: 14, Failures: 0, Errors: 0`

- [ ] **Step 5: Run all domain tests together**

```bash
mvn test -Dtest="DebitCreditNotePdfDocumentTest,DebitCreditNotePdfGenerationExceptionTest,PdfGenerationConstantsTest" -Dspring.profiles.active=test 2>&1 | tail -5
```

Expected: `Tests run: 18, Failures: 0, Errors: 0`

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add DebitCreditNotePdfDocument aggregate with state machine and tests"
```

---

## Task 5: Application output ports

**Note:** `DebitCreditNotePdfGenerationService` created in Task 3 has the wrong signature — update it now before writing the ports.

**Files:**
- Modify: `src/main/java/com/wpanther/debitcreditnote/pdf/domain/service/DebitCreditNotePdfGenerationService.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/PdfEventPort.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/PdfStoragePort.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/SagaReplyPort.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/port/out/SignedXmlFetchPort.java`

- [ ] **Step 1: Fix `DebitCreditNotePdfGenerationService.java` — add `documentNumber` parameter**

Replace the entire file with:

```java
package com.wpanther.debitcreditnote.pdf.domain.service;

public interface DebitCreditNotePdfGenerationService {

    byte[] generatePdf(String documentNumber, String signedXml);
}
```

- [ ] **Step 2: Create `PdfEventPort.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.port.out;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging.DebitCreditNotePdfGeneratedEvent;

public interface PdfEventPort {
    void publishPdfGenerated(DebitCreditNotePdfGeneratedEvent event);
}
```

- [ ] **Step 3: Create `PdfStoragePort.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.port.out;

public interface PdfStoragePort {
    /** Upload PDF bytes and return the S3 key. */
    String store(String documentNumber, byte[] pdfBytes);
    /** Delete a stored PDF by S3 key (best-effort). */
    void delete(String s3Key);
    /** Resolve a full URL from an S3 key. */
    String resolveUrl(String s3Key);
}
```

- [ ] **Step 4: Create `SagaReplyPort.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.port.out;

import com.wpanther.saga.domain.enums.SagaStep;

public interface SagaReplyPort {
    void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId,
                        String pdfUrl, long pdfSize);
    void publishFailure(String sagaId, SagaStep sagaStep, String correlationId,
                        String errorMessage);
    void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId);
}
```

- [ ] **Step 5: Create `SignedXmlFetchPort.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.port.out;

public interface SignedXmlFetchPort {

    String fetch(String url);

    class SignedXmlFetchException extends RuntimeException {
        public SignedXmlFetchException(String message) {
            super(message);
        }

        public SignedXmlFetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
```

- [ ] **Step 6: Verify compilation**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD FAILURE` — ports compile but `DebitCreditNotePdfGeneratedEvent` doesn't exist yet (referenced in `PdfEventPort`). That is correct for this stage.

- [ ] **Step 7: Commit**

```bash
git add src/
git commit -m "feat: add application output ports and fix domain service signature"
```

---

## Task 6: Kafka command DTOs + Camel route config

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteProcessCommand.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaDebitCreditNoteCompensateCommand.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java`
- Test: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java`

- [ ] **Step 1: Create `KafkaDebitCreditNoteProcessCommand.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

public class KafkaDebitCreditNoteProcessCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @Getter @JsonProperty("documentId")     private final String documentId;
    @Getter @JsonProperty("documentNumber") private final String documentNumber;
    @Getter @JsonProperty("signedXmlUrl")   private final String signedXmlUrl;

    @JsonCreator
    public KafkaDebitCreditNoteProcessCommand(
            @JsonProperty("eventId")        UUID eventId,
            @JsonProperty("occurredAt")     Instant occurredAt,
            @JsonProperty("eventType")      String eventType,
            @JsonProperty("version")        int version,
            @JsonProperty("sagaId")         String sagaId,
            @JsonProperty("sagaStep")       SagaStep sagaStep,
            @JsonProperty("correlationId")  String correlationId,
            @JsonProperty("documentId")     String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("signedXmlUrl")   String signedXmlUrl) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl   = signedXmlUrl;
    }

    /** Convenience constructor for testing. */
    public KafkaDebitCreditNoteProcessCommand(String sagaId, SagaStep sagaStep, String correlationId,
                                              String documentId, String documentNumber, String signedXmlUrl) {
        super(sagaId, sagaStep, correlationId);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl   = signedXmlUrl;
    }

    @Override public String getSagaId()        { return super.getSagaId(); }
    @Override public SagaStep getSagaStep()    { return super.getSagaStep(); }
    @Override public String getCorrelationId() { return super.getCorrelationId(); }
    public String getDocumentId()     { return documentId; }
    public String getDocumentNumber() { return documentNumber; }
    public String getSignedXmlUrl()   { return signedXmlUrl; }
}
```

- [ ] **Step 2: Create `KafkaDebitCreditNoteCompensateCommand.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

public class KafkaDebitCreditNoteCompensateCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @Getter
    @JsonProperty("documentId") private final String documentId;

    @JsonCreator
    public KafkaDebitCreditNoteCompensateCommand(
            @JsonProperty("eventId")       UUID eventId,
            @JsonProperty("occurredAt")    Instant occurredAt,
            @JsonProperty("eventType")     String eventType,
            @JsonProperty("version")       int version,
            @JsonProperty("sagaId")        String sagaId,
            @JsonProperty("sagaStep")      SagaStep sagaStep,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("documentId")    String documentId) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId = documentId;
    }

    /** Convenience constructor for testing. */
    public KafkaDebitCreditNoteCompensateCommand(String sagaId, SagaStep sagaStep, String correlationId,
                                                 String documentId) {
        super(sagaId, sagaStep, correlationId);
        this.documentId = documentId;
    }

    @Override public String getSagaId()        { return super.getSagaId(); }
    @Override public SagaStep getSagaStep()    { return super.getSagaStep(); }
    @Override public String getCorrelationId() { return super.getCorrelationId(); }
    public String getDocumentId() { return documentId; }
}
```

- [ ] **Step 3: Create `KafkaCommandMapper.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import org.springframework.stereotype.Component;

@Component
public class KafkaCommandMapper {

    public KafkaDebitCreditNoteProcessCommand toProcess(KafkaDebitCreditNoteProcessCommand src) {
        return src;
    }

    public KafkaDebitCreditNoteCompensateCommand toCompensate(KafkaDebitCreditNoteCompensateCommand src) {
        return src;
    }
}
```

- [ ] **Step 4: Create `SagaRouteConfig.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.debitcreditnote.pdf.application.service.SagaCommandHandler;
import com.wpanther.debitcreditnote.pdf.application.usecase.CompensateDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.application.usecase.ProcessDebitCreditNotePdfUseCase;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class SagaRouteConfig extends RouteBuilder {

    private final ProcessDebitCreditNotePdfUseCase processUseCase;
    private final CompensateDebitCreditNotePdfUseCase compensateUseCase;
    private final SagaCommandHandler sagaCommandHandler;
    private final ObjectMapper objectMapper;

    public SagaRouteConfig(ProcessDebitCreditNotePdfUseCase processUseCase,
                           CompensateDebitCreditNotePdfUseCase compensateUseCase,
                           SagaCommandHandler sagaCommandHandler,
                           ObjectMapper objectMapper) {
        this.processUseCase     = processUseCase;
        this.compensateUseCase  = compensateUseCase;
        this.sagaCommandHandler = sagaCommandHandler;
        this.objectMapper       = objectMapper;
    }

    @Override
    public void configure() throws Exception {

        errorHandler(deadLetterChannel(
                        "kafka:{{app.kafka.topics.dlq}}?brokers={{app.kafka.bootstrap-servers}}")
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2)
                        .maximumRedeliveryDelay(10000)
                        .logExhausted(true)
                        .logStackTrace(true)
                        .onPrepareFailure(exchange -> {
                            Throwable cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
                            Object body = exchange.getIn().getBody();
                            if (body instanceof KafkaDebitCreditNoteProcessCommand cmd) {
                                log.error("DLQ: notifying orchestrator of retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentNumber());
                                sagaCommandHandler.publishOrchestrationFailure(cmd, cause);
                            } else if (body instanceof KafkaDebitCreditNoteCompensateCommand cmd) {
                                log.error("DLQ: notifying orchestrator of compensation retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentId());
                                sagaCommandHandler.publishCompensationOrchestrationFailure(cmd, cause);
                            } else {
                                log.error("DLQ: body not deserialized ({}); attempting saga metadata recovery",
                                        body == null ? "null" : body.getClass().getSimpleName());
                                recoverAndNotifyOrchestrator(body, cause);
                            }
                        }));

        from("kafka:{{app.kafka.topics.saga-command-debit-credit-note-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.command-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-command-consumer")
                .unmarshal().json(JsonLibrary.Jackson, KafkaDebitCreditNoteProcessCommand.class)
                .process(exchange -> {
                        KafkaDebitCreditNoteProcessCommand cmd =
                                exchange.getIn().getBody(KafkaDebitCreditNoteProcessCommand.class);
                        log.info("Processing saga command for saga: {}, document: {}",
                                        cmd.getSagaId(), cmd.getDocumentNumber());
                        processUseCase.handle(cmd);
                })
                .log("Successfully processed saga command");

        from("kafka:{{app.kafka.topics.saga-compensation-debit-credit-note-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.compensation-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-compensation-consumer")
                .unmarshal().json(JsonLibrary.Jackson, KafkaDebitCreditNoteCompensateCommand.class)
                .process(exchange -> {
                        KafkaDebitCreditNoteCompensateCommand cmd =
                                exchange.getIn().getBody(KafkaDebitCreditNoteCompensateCommand.class);
                        log.info("Processing compensation for saga: {}, document: {}",
                                        cmd.getSagaId(), cmd.getDocumentId());
                        compensateUseCase.handle(cmd);
                })
                .log("Successfully processed compensation command");
    }

    private void recoverAndNotifyOrchestrator(Object body, Throwable cause) {
        if (body == null) {
            log.error("DLQ: null message body — orchestrator must timeout");
            return;
        }
        try {
            byte[] rawBytes = body instanceof byte[] b
                    ? b
                    : body.toString().getBytes(StandardCharsets.UTF_8);
            JsonNode node        = objectMapper.readTree(rawBytes);
            String sagaId        = node.path("sagaId").asText(null);
            String sagaStepStr   = node.path("sagaStep").asText(null);
            String correlationId = node.path("correlationId").asText(null);

            if (sagaId == null || sagaStepStr == null) {
                log.error("DLQ: saga metadata missing in raw message — orchestrator must timeout");
                return;
            }
            SagaStep sagaStep = objectMapper.readValue("\"" + sagaStepStr + "\"", SagaStep.class);
            sagaCommandHandler.publishOrchestrationFailureForUnparsedMessage(
                    sagaId, sagaStep, correlationId, cause);
        } catch (Exception parseEx) {
            log.error("DLQ: cannot parse raw message for saga metadata — orchestrator must timeout", parseEx);
        }
    }
}
```

- [ ] **Step 5: Write `KafkaCommandMapperTest.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.saga.domain.enums.SagaStep;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaCommandMapperTest {

    private final KafkaCommandMapper mapper = new KafkaCommandMapper();

    @Test
    void toProcess_returnsSameInstance() {
        var cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "doc-1", "DCN-2024-001", "http://storage/signed.xml");
        assertThat(mapper.toProcess(cmd)).isSameAs(cmd);
    }

    @Test
    void toCompensate_returnsSameInstance() {
        var cmd = new KafkaDebitCreditNoteCompensateCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1", "doc-1");
        assertThat(mapper.toCompensate(cmd)).isSameAs(cmd);
    }

    @Test
    void processCommand_fieldsPreserved() {
        var cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "doc-1", "DCN-2024-001", "http://storage/signed.xml");
        assertThat(cmd.getSagaId()).isEqualTo("saga-1");
        assertThat(cmd.getDocumentId()).isEqualTo("doc-1");
        assertThat(cmd.getDocumentNumber()).isEqualTo("DCN-2024-001");
        assertThat(cmd.getSignedXmlUrl()).isEqualTo("http://storage/signed.xml");
        assertThat(cmd.getSagaStep()).isEqualTo(SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF);
    }

    @Test
    void compensateCommand_fieldsPreserved() {
        var cmd = new KafkaDebitCreditNoteCompensateCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1", "doc-1");
        assertThat(cmd.getSagaId()).isEqualTo("saga-1");
        assertThat(cmd.getDocumentId()).isEqualTo("doc-1");
        assertThat(cmd.getCorrelationId()).isEqualTo("corr-1");
    }
}
```

- [ ] **Step 6: Run the mapper test**

```bash
mvn test -Dtest=KafkaCommandMapperTest -Dspring.profiles.active=test 2>&1 | tail -5
```

Expected: `Tests run: 4, Failures: 0, Errors: 0`

- [ ] **Step 7: Commit**

```bash
git add src/
git commit -m "feat: add Kafka command DTOs and Camel route config"
```

---

## Task 7: Messaging — outbox events + publishers

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/OutboxConstants.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/DebitCreditNotePdfGeneratedEvent.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/DebitCreditNotePdfReplyEvent.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/EventPublisher.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java`

- [ ] **Step 1: Create `OutboxConstants.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

final class OutboxConstants {
    static final String AGGREGATE_TYPE = "DebitCreditNotePdfDocument";
    private OutboxConstants() {}
}
```

- [ ] **Step 2: Create `DebitCreditNotePdfGeneratedEvent.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.model.TraceEvent;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Getter
public class DebitCreditNotePdfGeneratedEvent extends TraceEvent {

    private static final String EVENT_TYPE  = "pdf.generated.debit-credit-note";
    private static final String SOURCE      = "debitcreditnote-pdf-generation-service";
    private static final String TRACE_TYPE  = "PDF_GENERATED";

    @JsonProperty("documentId")     private final String documentId;
    @JsonProperty("documentNumber") private final String documentNumber;
    @JsonProperty("documentUrl")    private final String documentUrl;
    @JsonProperty("fileSize")       private final long fileSize;
    @JsonProperty("xmlEmbedded")    private final boolean xmlEmbedded;

    public DebitCreditNotePdfGeneratedEvent(
            String sagaId, String documentId, String documentNumber,
            String documentUrl, long fileSize, boolean xmlEmbedded, String correlationId) {
        super(sagaId, correlationId, SOURCE, TRACE_TYPE, null);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.documentUrl    = documentUrl;
        this.fileSize       = fileSize;
        this.xmlEmbedded    = xmlEmbedded;
    }

    @Override
    public String getEventType() { return EVENT_TYPE; }

    @JsonCreator
    public DebitCreditNotePdfGeneratedEvent(
            @JsonProperty("eventId")        UUID eventId,
            @JsonProperty("occurredAt")     Instant occurredAt,
            @JsonProperty("eventType")      String eventType,
            @JsonProperty("version")        int version,
            @JsonProperty("sagaId")         String sagaId,
            @JsonProperty("correlationId")  String correlationId,
            @JsonProperty("source")         String source,
            @JsonProperty("traceType")      String traceType,
            @JsonProperty("context")        String context,
            @JsonProperty("documentId")     String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("documentUrl")    String documentUrl,
            @JsonProperty("fileSize")       long fileSize,
            @JsonProperty("xmlEmbedded")    boolean xmlEmbedded) {
        super(eventId, occurredAt, eventType, version, sagaId, correlationId, source, traceType, context);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.documentUrl    = documentUrl;
        this.fileSize       = fileSize;
        this.xmlEmbedded    = xmlEmbedded;
    }
}
```

- [ ] **Step 3: Create `DebitCreditNotePdfReplyEvent.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaReply;

public class DebitCreditNotePdfReplyEvent extends SagaReply {

    private static final long serialVersionUID = 1L;

    private String pdfUrl;
    private Long pdfSize;

    public static DebitCreditNotePdfReplyEvent success(
            String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, Long pdfSize) {
        DebitCreditNotePdfReplyEvent reply =
                new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.SUCCESS);
        reply.pdfUrl  = pdfUrl;
        reply.pdfSize = pdfSize;
        return reply;
    }

    public static DebitCreditNotePdfReplyEvent failure(
            String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        return new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, errorMessage);
    }

    public static DebitCreditNotePdfReplyEvent compensated(
            String sagaId, SagaStep sagaStep, String correlationId) {
        return new DebitCreditNotePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.COMPENSATED);
    }

    private DebitCreditNotePdfReplyEvent(String sagaId, SagaStep sagaStep,
                                         String correlationId, ReplyStatus status) {
        super(sagaId, sagaStep, correlationId, status);
    }

    private DebitCreditNotePdfReplyEvent(String sagaId, SagaStep sagaStep,
                                         String correlationId, String errorMessage) {
        super(sagaId, sagaStep, correlationId, errorMessage);
    }

    public String getPdfUrl()  { return pdfUrl; }
    public Long getPdfSize()   { return pdfSize; }
}
```

- [ ] **Step 4: Create `EventPublisher.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class EventPublisher implements PdfEventPort {

    private static final String AGGREGATE_TYPE = OutboxConstants.AGGREGATE_TYPE;

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishPdfGenerated(DebitCreditNotePdfGeneratedEvent event) {
        Map<String, String> headers = Map.of(
            "documentType", "DEBIT_CREDIT_NOTE",
            "correlationId", event.getCorrelationId()
        );
        outboxService.saveWithRouting(
            event, AGGREGATE_TYPE, event.getDocumentId(),
            "pdf.generated.debit-credit-note", event.getDocumentId(), toJson(headers));
        log.info("Published DebitCreditNotePdfGeneratedEvent to outbox: {}", event.getDocumentNumber());
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event headers", e);
        }
    }
}
```

- [ ] **Step 5: Create `SagaReplyPublisher.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaReplyPublisher implements SagaReplyPort {

    private static final String REPLY_TOPIC    = "saga.reply.debit-credit-note-pdf";
    private static final String AGGREGATE_TYPE = OutboxConstants.AGGREGATE_TYPE;

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId,
                               String pdfUrl, long pdfSize) {
        DebitCreditNotePdfReplyEvent reply =
                DebitCreditNotePdfReplyEvent.success(sagaId, sagaStep, correlationId, pdfUrl, pdfSize);
        outboxService.saveWithRouting(reply, AGGREGATE_TYPE, sagaId, REPLY_TOPIC, sagaId,
                toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "SUCCESS")));
        log.info("Published SUCCESS saga reply for saga {} step {}", sagaId, sagaStep);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        DebitCreditNotePdfReplyEvent reply =
                DebitCreditNotePdfReplyEvent.failure(sagaId, sagaStep, correlationId, errorMessage);
        outboxService.saveWithRouting(reply, AGGREGATE_TYPE, sagaId, REPLY_TOPIC, sagaId,
                toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "FAILURE")));
        log.info("Published FAILURE saga reply for saga {} step {}: {}", sagaId, sagaStep, errorMessage);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId) {
        DebitCreditNotePdfReplyEvent reply =
                DebitCreditNotePdfReplyEvent.compensated(sagaId, sagaStep, correlationId);
        outboxService.saveWithRouting(reply, AGGREGATE_TYPE, sagaId, REPLY_TOPIC, sagaId,
                toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "COMPENSATED")));
        log.info("Published COMPENSATED saga reply for saga {} step {}", sagaId, sagaStep);
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event headers", e);
        }
    }
}
```

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add messaging events and outbox publishers"
```

---

## Task 8: Persistence layer — outbox + document entity + repository adapter

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/outbox/OutboxEventEntity.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/outbox/SpringDataOutboxRepository.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/outbox/JpaOutboxEventRepository.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/DebitCreditNotePdfDocumentEntity.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/JpaDebitCreditNotePdfDocumentRepository.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/DebitCreditNotePdfDocumentRepositoryAdapter.java`
- Test: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/JpaDebitCreditNotePdfDocumentRepositoryImplTest.java`

- [ ] **Step 1: Copy outbox infrastructure — `OutboxEventEntity.java`**

Copy `taxinvoice-pdf-generation-service/.../persistence/outbox/OutboxEventEntity.java`, changing only the package declaration:

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.outbox;

import com.wpanther.saga.domain.outbox.OutboxStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events", indexes = {
    @Index(name = "idx_outbox_status", columnList = "status"),
    @Index(name = "idx_outbox_created", columnList = "created_at"),
    @Index(name = "idx_outbox_aggregate", columnList = "aggregate_id, aggregate_type")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OutboxEventEntity {

    @Id @Column(name = "id", nullable = false) private UUID id;
    @Column(name = "aggregate_type", nullable = false, length = 100) private String aggregateType;
    @Column(name = "aggregate_id", nullable = false, length = 100) private String aggregateId;
    @Column(name = "event_type", nullable = false, length = 100) private String eventType;
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT") private String payload;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
    @Column(name = "published_at") private Instant publishedAt;
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20) private OutboxStatus status;
    @Column(name = "retry_count") private Integer retryCount;
    @Column(name = "error_message", length = 1000) private String errorMessage;
    @Column(name = "topic", length = 255) private String topic;
    @Column(name = "partition_key", length = 255) private String partitionKey;
    @Column(name = "headers", columnDefinition = "TEXT") private String headers;

    @PrePersist
    protected void onCreate() {
        if (id == null)          id = UUID.randomUUID();
        if (status == null)      status = OutboxStatus.PENDING;
        if (createdAt == null)   createdAt = Instant.now();
        if (retryCount == null)  retryCount = 0;
    }

    public static OutboxEventEntity fromDomain(com.wpanther.saga.domain.outbox.OutboxEvent event) {
        return OutboxEventEntity.builder()
                .id(event.getId()).aggregateType(event.getAggregateType())
                .aggregateId(event.getAggregateId()).eventType(event.getEventType())
                .payload(event.getPayload()).createdAt(event.getCreatedAt())
                .publishedAt(event.getPublishedAt()).status(event.getStatus())
                .retryCount(event.getRetryCount()).errorMessage(event.getErrorMessage())
                .topic(event.getTopic()).partitionKey(event.getPartitionKey())
                .headers(event.getHeaders()).build();
    }

    public com.wpanther.saga.domain.outbox.OutboxEvent toDomain() {
        return com.wpanther.saga.domain.outbox.OutboxEvent.builder()
                .id(this.id).aggregateType(this.aggregateType).aggregateId(this.aggregateId)
                .eventType(this.eventType).payload(this.payload).createdAt(this.createdAt)
                .publishedAt(this.publishedAt).status(this.status).retryCount(this.retryCount)
                .errorMessage(this.errorMessage).topic(this.topic).partitionKey(this.partitionKey)
                .headers(this.headers).build();
    }
}
```

- [ ] **Step 2: Create `SpringDataOutboxRepository.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.outbox;

import com.wpanther.saga.domain.outbox.OutboxStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface SpringDataOutboxRepository extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(OutboxStatus status, Pageable pageable);

    @Query("SELECT e FROM OutboxEventEntity e WHERE e.status = 'FAILED' ORDER BY e.createdAt ASC")
    List<OutboxEventEntity> findFailedEventsOrderByCreatedAtAsc(Pageable pageable);

    List<OutboxEventEntity> findByAggregateTypeAndAggregateIdOrderByCreatedAtAsc(
            String aggregateType, String aggregateId);

    @Modifying
    @Query("DELETE FROM OutboxEventEntity e WHERE e.status = 'PUBLISHED' AND e.publishedAt < :before")
    int deletePublishedBefore(@Param("before") Instant before);
}
```

- [ ] **Step 3: Create `JpaOutboxEventRepository.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.outbox;

import com.wpanther.saga.domain.outbox.OutboxEvent;
import com.wpanther.saga.domain.outbox.OutboxEventRepository;
import com.wpanther.saga.domain.outbox.OutboxStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class JpaOutboxEventRepository implements OutboxEventRepository {

    private static final Logger log = LoggerFactory.getLogger(JpaOutboxEventRepository.class);
    private final SpringDataOutboxRepository springRepository;

    public JpaOutboxEventRepository(SpringDataOutboxRepository springRepository) {
        this.springRepository = springRepository;
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
        return springRepository.save(OutboxEventEntity.fromDomain(event)).toDomain();
    }

    @Override
    public Optional<OutboxEvent> findById(UUID id) {
        return springRepository.findById(id).map(OutboxEventEntity::toDomain);
    }

    @Override
    public List<OutboxEvent> findPendingEvents(int limit) {
        return springRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING, Pageable.ofSize(limit))
                .stream().map(OutboxEventEntity::toDomain).toList();
    }

    @Override
    public List<OutboxEvent> findFailedEvents(int limit) {
        return springRepository.findFailedEventsOrderByCreatedAtAsc(Pageable.ofSize(limit))
                .stream().map(OutboxEventEntity::toDomain).toList();
    }

    @Override
    public int deletePublishedBefore(Instant before) {
        return springRepository.deletePublishedBefore(before);
    }

    @Override
    public List<OutboxEvent> findByAggregate(String aggregateType, String aggregateId) {
        return springRepository.findByAggregateTypeAndAggregateIdOrderByCreatedAtAsc(aggregateType, aggregateId)
                .stream().map(OutboxEventEntity::toDomain).toList();
    }
}
```

- [ ] **Step 4: Create `DebitCreditNotePdfDocumentEntity.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "debit_credit_note_pdf_documents", indexes = {
    @Index(name = "idx_dcn_pdf_dcn_id",     columnList = "debit_credit_note_id"),
    @Index(name = "idx_dcn_pdf_doc_number",  columnList = "document_number"),
    @Index(name = "idx_dcn_pdf_status",      columnList = "status")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class DebitCreditNotePdfDocumentEntity {

    @Id @Column(name = "id", nullable = false) private UUID id;

    @Column(name = "debit_credit_note_id", nullable = false, length = 100) private String debitCreditNoteId;
    @Column(name = "document_number",      nullable = false, length = 50)  private String documentNumber;
    @Column(name = "document_path",  length = 500)  private String documentPath;
    @Column(name = "document_url",   length = 1000) private String documentUrl;
    @Column(name = "file_size")                     private Long fileSize;
    @Column(name = "mime_type", nullable = false, length = 100) private String mimeType;
    @Column(name = "xml_embedded", nullable = false)            private Boolean xmlEmbedded;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20) private GenerationStatus status;

    @Column(name = "error_message", columnDefinition = "TEXT") private String errorMessage;
    @Column(name = "retry_count")                              private Integer retryCount;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false) private LocalDateTime createdAt;
    @Column(name = "completed_at") private LocalDateTime completedAt;

    @PrePersist
    protected void onCreate() {
        if (id == null)          id = UUID.randomUUID();
        if (status == null)      status = GenerationStatus.PENDING;
        if (mimeType == null)    mimeType = "application/pdf";
        if (xmlEmbedded == null) xmlEmbedded = false;
        if (retryCount == null)  retryCount = 0;
    }
}
```

- [ ] **Step 5: Create `JpaDebitCreditNotePdfDocumentRepository.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public interface JpaDebitCreditNotePdfDocumentRepository
        extends JpaRepository<DebitCreditNotePdfDocumentEntity, UUID> {

    Optional<DebitCreditNotePdfDocumentEntity> findByDebitCreditNoteId(String debitCreditNoteId);

    @Query("SELECT e.documentPath FROM DebitCreditNotePdfDocumentEntity e WHERE e.documentPath IS NOT NULL")
    Set<String> findAllDocumentPaths();
}
```

- [ ] **Step 6: Create `DebitCreditNotePdfDocumentRepositoryAdapter.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class DebitCreditNotePdfDocumentRepositoryAdapter implements DebitCreditNotePdfDocumentRepository {

    private final JpaDebitCreditNotePdfDocumentRepository jpaRepository;

    @Override
    public DebitCreditNotePdfDocument save(DebitCreditNotePdfDocument document) {
        return toDomain(jpaRepository.save(toEntity(document)));
    }

    @Override
    public Optional<DebitCreditNotePdfDocument> findById(UUID id) {
        return jpaRepository.findById(id).map(this::toDomain);
    }

    @Override
    public Optional<DebitCreditNotePdfDocument> findByDebitCreditNoteId(String debitCreditNoteId) {
        return jpaRepository.findByDebitCreditNoteId(debitCreditNoteId).map(this::toDomain);
    }

    @Override
    public void deleteById(UUID id) { jpaRepository.deleteById(id); }

    @Override
    public void flush() { jpaRepository.flush(); }

    private DebitCreditNotePdfDocumentEntity toEntity(DebitCreditNotePdfDocument d) {
        return DebitCreditNotePdfDocumentEntity.builder()
            .id(d.getId()).debitCreditNoteId(d.getDebitCreditNoteId())
            .documentNumber(d.getDocumentNumber()).documentPath(d.getDocumentPath())
            .documentUrl(d.getDocumentUrl()).fileSize(d.getFileSize())
            .mimeType(d.getMimeType()).xmlEmbedded(d.isXmlEmbedded())
            .status(d.getStatus()).errorMessage(d.getErrorMessage())
            .retryCount(d.getRetryCount()).createdAt(d.getCreatedAt())
            .completedAt(d.getCompletedAt()).build();
    }

    private DebitCreditNotePdfDocument toDomain(DebitCreditNotePdfDocumentEntity e) {
        return DebitCreditNotePdfDocument.builder()
            .id(e.getId()).debitCreditNoteId(e.getDebitCreditNoteId())
            .documentNumber(e.getDocumentNumber()).documentPath(e.getDocumentPath())
            .documentUrl(e.getDocumentUrl())
            .fileSize(e.getFileSize() != null ? e.getFileSize() : 0L)
            .mimeType(e.getMimeType())
            .xmlEmbedded(e.getXmlEmbedded() != null && e.getXmlEmbedded())
            .status(e.getStatus()).errorMessage(e.getErrorMessage())
            .retryCount(e.getRetryCount() != null ? e.getRetryCount() : 0)
            .createdAt(e.getCreatedAt()).completedAt(e.getCompletedAt()).build();
    }
}
```

- [ ] **Step 7: Write persistence integration test**

Create `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/persistence/JpaDebitCreditNotePdfDocumentRepositoryImplTest.java`:

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

@DataJpaTest
@ActiveProfiles("test")
@Import(DebitCreditNotePdfDocumentRepositoryAdapter.class)
class JpaDebitCreditNotePdfDocumentRepositoryImplTest {

    @Autowired
    private DebitCreditNotePdfDocumentRepository repository;

    @Test
    void saveAndFindById() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001")
                .documentNumber("DCN-2024-001")
                .build();
        DebitCreditNotePdfDocument saved = repository.save(doc);

        Optional<DebitCreditNotePdfDocument> found = repository.findById(saved.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getDebitCreditNoteId()).isEqualTo("dcn-001");
        assertThat(found.get().getStatus()).isEqualTo(GenerationStatus.PENDING);
    }

    @Test
    void findByDebitCreditNoteId_found() {
        repository.save(DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-002").documentNumber("DCN-2024-002").build());

        Optional<DebitCreditNotePdfDocument> found = repository.findByDebitCreditNoteId("dcn-002");
        assertThat(found).isPresent();
        assertThat(found.get().getDocumentNumber()).isEqualTo("DCN-2024-002");
    }

    @Test
    void findByDebitCreditNoteId_notFound() {
        assertThat(repository.findByDebitCreditNoteId("nonexistent")).isEmpty();
    }

    @Test
    void deleteById_removesDocument() {
        DebitCreditNotePdfDocument doc = repository.save(
                DebitCreditNotePdfDocument.builder()
                        .debitCreditNoteId("dcn-003").documentNumber("DCN-2024-003").build());
        repository.deleteById(doc.getId());
        assertThat(repository.findById(doc.getId())).isEmpty();
    }
}
```

- [ ] **Step 8: Run the persistence test**

```bash
mvn test -Dtest=JpaDebitCreditNotePdfDocumentRepositoryImplTest -Dspring.profiles.active=test 2>&1 | tail -10
```

Expected: `Tests run: 4, Failures: 0, Errors: 0`

- [ ] **Step 9: Commit**

```bash
git add src/
git commit -m "feat: add persistence layer — entity, JPA repos, outbox infrastructure"
```

---

## Task 9: PDF generation — ThaiAmountWordsConverter, FopGenerator, PdfA3Converter, ServiceImpl

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverter.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/FopDebitCreditNotePdfGenerator.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/PdfA3Converter.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/DebitCreditNotePdfGenerationServiceImpl.java`

- [ ] **Step 1: Copy `ThaiAmountWordsConverter.java`** (no changes except package)

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class ThaiAmountWordsConverter {

    private static final String[] DIGITS =
        {"ศูนย์", "หนึ่ง", "สอง", "สาม", "สี่", "ห้า", "หก", "เจ็ด", "แปด", "เก้า"};

    private ThaiAmountWordsConverter() {}

    public static String toWords(BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Amount must be non-negative");
        }
        amount = amount.setScale(2, RoundingMode.HALF_UP);
        long totalSatang = amount.movePointRight(2).longValue();
        long baht   = totalSatang / 100;
        long satang = totalSatang % 100;

        StringBuilder result = new StringBuilder(numberToThai(baht));
        if (satang == 0) {
            result.append("บาทถ้วน");
        } else {
            result.append("บาท");
            result.append(numberToThai(satang));
            result.append("สตางค์");
        }
        return result.toString();
    }

    private static String numberToThai(long n) {
        if (n == 0) return "ศูนย์";
        StringBuilder sb = new StringBuilder();
        if (n >= 1_000_000) {
            sb.append(numberToThai(n / 1_000_000));
            sb.append("ล้าน");
            n %= 1_000_000;
            if (n == 0) return sb.toString();
        }
        boolean hasTens = (n % 100) >= 10;
        int[]    place  = {100_000, 10_000, 1_000, 100, 10, 1};
        String[] unit   = {"แสน",   "หมื่น", "พัน", "ร้อย", "สิบ", ""};
        for (int i = 0; i < place.length; i++) {
            int digit = (int) (n / place[i]);
            n %= place[i];
            if (digit == 0) continue;
            if (i == 4) {
                if (digit == 1)      sb.append("สิบ");
                else if (digit == 2) sb.append("ยี่สิบ");
                else                 sb.append(DIGITS[digit]).append("สิบ");
            } else if (i == 5) {
                if (digit == 1 && hasTens) sb.append("เอ็ด");
                else                        sb.append(DIGITS[digit]);
            } else {
                sb.append(DIGITS[digit]).append(unit[i]);
            }
        }
        return sb.toString();
    }
}
```

- [ ] **Step 2: Create `FopDebitCreditNotePdfGenerator.java`**

This is an exact copy of `FopTaxInvoicePdfGenerator` with these substitutions:
- Package: `com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf`
- Class name: `FopDebitCreditNotePdfGenerator` (all references)
- XSL constant: `private static final String DEBITCREDITNOTE_XSL_PATH = "xsl/debitcreditnote-direct.xsl";`  (was `TAXINVOICE_XSL_PATH`)
- `compileTemplates(tf, DEBITCREDITNOTE_XSL_PATH)` (was `TAXINVOICE_XSL_PATH`)
- Log messages: replace "Tax Invoice" with "Debit/Credit Note" and "FopTaxInvoicePdfGenerator" with "FopDebitCreditNotePdfGenerator"
- Inner exception classes: `PdfGenerationException` and `PdfInitializationException` — keep names the same

Full file:

```java
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
```

- [ ] **Step 3: Copy `PdfA3Converter.java`** (only package changes)

Copy `taxinvoice-pdf-generation-service/.../pdf/PdfA3Converter.java` verbatim, changing only:
- Line 1 package: `package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;`
- In `addPdfAMetadata`: change `"Thai e-Tax Tax Invoice: " + taxInvoiceNumber` to `"Thai e-Tax Debit/Credit Note: " + taxInvoiceNumber` and `"Tax Invoice PDF Generation Service"` to `"Debit/Credit Note PDF Generation Service"` and `"Thai e-Tax Tax Invoice System"` to `"Thai e-Tax Debit/Credit Note System"`

The method signature `convertToPdfA3(byte[] pdfBytes, String xmlContent, String xmlFilename, String taxInvoiceNumber)` keeps the `taxInvoiceNumber` param name — that is fine, it is just a local variable name.

- [ ] **Step 4: Create `DebitCreditNotePdfGenerationServiceImpl.java`**

```java
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
```

- [ ] **Step 5: Commit**

```bash
git add src/
git commit -m "feat: add PDF generation pipeline — FOP generator, PDF/A-3 converter, service impl"
```

---

## Task 10: Storage + REST client + config + metrics

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/storage/MinioStorageAdapter.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/storage/MinioCleanupService.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcher.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/MinioConfig.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/OutboxConfig.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/RestTemplateConfig.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/FontHealthCheck.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/infrastructure/metrics/PdfGenerationMetrics.java`

- [ ] **Step 1: Create `MinioStorageAdapter.java`**

Copy `taxinvoice-pdf-generation-service/.../storage/MinioStorageAdapter.java`, changing:
- Package: `com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.storage`
- In `doStore`: change `"taxinvoice-%s-%s.pdf"` to `"debitcreditnote-%s-%s.pdf"`

Full `doStore` method (only change from taxinvoice):

```java
private String doStore(String documentNumber, byte[] pdfBytes) {
    LocalDate now = LocalDate.now();
    String safeName = sanitizeFilename(documentNumber);
    String fileName = String.format("debitcreditnote-%s-%s.pdf", safeName, UUID.randomUUID());
    String s3Key = String.format("%04d/%02d/%02d/%s",
            now.getYear(), now.getMonthValue(), now.getDayOfMonth(), fileName);

    PutObjectRequest put = PutObjectRequest.builder()
            .bucket(bucketName).key(s3Key)
            .contentType("application/pdf")
            .contentLength((long) pdfBytes.length).build();

    s3Client.putObject(put, RequestBody.fromBytes(pdfBytes));
    log.debug("Uploaded PDF to MinIO: bucket={}, key={}", bucketName, s3Key);
    return s3Key;
}
```

The full class is identical to `MinioStorageAdapter` in taxinvoice except for the package declaration and the `doStore` method above. The constructor, `store`, `delete`, `resolveUrl`, `listAllPdfs`, `deleteWithoutCircuitBreaker`, and `sanitizeFilename` methods are all identical.

- [ ] **Step 2: Create `MinioCleanupService.java`**

Copy `taxinvoice-pdf-generation-service/.../storage/MinioCleanupService.java`, changing:
- Package: `com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.storage`
- Import: `JpaDebitCreditNotePdfDocumentRepository` instead of `JpaTaxInvoicePdfDocumentRepository`
- Field type: `JpaDebitCreditNotePdfDocumentRepository repository`

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.storage;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.JpaDebitCreditNotePdfDocumentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.minio.cleanup.enabled", havingValue = "true", matchIfMissing = false)
public class MinioCleanupService {

    private final MinioStorageAdapter minioStorage;
    private final JpaDebitCreditNotePdfDocumentRepository repository;

    @Scheduled(cron = "${app.minio.cleanup.cron:0 0 2 * * ?}")
    public void cleanupOrphanedPdfs() {
        log.info("Starting orphaned PDF cleanup job");
        try {
            List<String> minioKeys    = minioStorage.listAllPdfs();
            Set<String>  databaseKeys = repository.findAllDocumentPaths();
            List<String> orphanedKeys = minioKeys.stream()
                    .filter(key -> !databaseKeys.contains(key)).toList();
            if (orphanedKeys.isEmpty()) { log.info("No orphaned PDFs found"); return; }
            log.warn("Found {} orphaned PDF(s) to delete: {}", orphanedKeys.size(), orphanedKeys);
            int deleted = 0;
            for (String key : orphanedKeys) { minioStorage.deleteWithoutCircuitBreaker(key); deleted++; }
            log.info("Orphaned PDF cleanup completed: {} deleted", deleted);
        } catch (Exception e) {
            log.error("Orphaned PDF cleanup job failed: {}", e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 3: Create `RestTemplateSignedXmlFetcher.java`**

Copy `taxinvoice-pdf-generation-service/.../client/RestTemplateSignedXmlFetcher.java`, changing:
- Package: `com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.client`
- Import: `com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort`

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.client;

import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort.SignedXmlFetchException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
@RequiredArgsConstructor
@Slf4j
public class RestTemplateSignedXmlFetcher implements SignedXmlFetchPort {

    private final RestTemplate restTemplate;

    @Override
    @CircuitBreaker(name = "signedXmlFetch", fallbackMethod = "fallbackOnFailure")
    public String fetch(String signedXmlUrl) {
        log.debug("Fetching signed XML from {}", signedXmlUrl);
        String response = restTemplate.getForObject(signedXmlUrl, String.class);
        if (response == null || response.isBlank()) {
            throw new IllegalStateException(
                    "Received null or empty signed XML response from: " + signedXmlUrl);
        }
        log.debug("Successfully fetched signed XML, size: {} bytes", response.length());
        return response;
    }

    private String fallbackOnFailure(String signedXmlUrl, Throwable throwable) {
        throw new SignedXmlFetchException(
                "Circuit breaker 'signedXmlFetch' is OPEN — " +
                "document-storage-service is degraded. URL: " + signedXmlUrl, throwable);
    }
}
```

- [ ] **Step 4: Create `MinioConfig.java`**

Copy `taxinvoice-pdf-generation-service/.../config/MinioConfig.java`, changing only the package:

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.config;
// rest of the file is identical to taxinvoice MinioConfig
```

(Keep all annotations and the `s3Client` `@Bean` method unchanged.)

- [ ] **Step 5: Create `OutboxConfig.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.outbox.JpaOutboxEventRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.outbox.SpringDataOutboxRepository;
import com.wpanther.saga.domain.outbox.OutboxEventRepository;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OutboxConfig {

    @Bean
    @ConditionalOnMissingBean(OutboxEventRepository.class)
    public OutboxEventRepository outboxEventRepository(SpringDataOutboxRepository springRepository) {
        return new JpaOutboxEventRepository(springRepository);
    }

    @Bean
    @ConditionalOnMissingBean(OutboxService.class)
    public OutboxService outboxService(OutboxEventRepository repository, ObjectMapper objectMapper) {
        return new OutboxService(repository, objectMapper);
    }
}
```

- [ ] **Step 6: Create `RestTemplateConfig.java`**

Copy `taxinvoice-pdf-generation-service/.../config/RestTemplateConfig.java`, changing only the package to `com.wpanther.debitcreditnote.pdf.infrastructure.config`. All other code is identical.

- [ ] **Step 7: Create `FontHealthCheck.java`**

Copy `taxinvoice-pdf-generation-service/.../config/FontHealthCheck.java`, changing only the package to `com.wpanther.debitcreditnote.pdf.infrastructure.config`. All other code is identical (font paths, behaviour, log messages).

- [ ] **Step 8: Create `PdfGenerationMetrics.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class PdfGenerationMetrics {

    private static final String RETRY_EXHAUSTED_COUNTER = "pdf.generation.retry.exhausted";
    private static final String TAG_SAGA_ID             = "saga_id";
    private static final String TAG_DCN_ID              = "debit_credit_note_id";
    private static final String TAG_DCN_NUMBER          = "document_number";

    private final MeterRegistry meterRegistry;

    public PdfGenerationMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        Counter.builder(RETRY_EXHAUSTED_COUNTER)
                .description("Number of times PDF generation max retries were exceeded")
                .register(meterRegistry);
    }

    public void recordRetryExhausted(String sagaId, String debitCreditNoteId, String documentNumber) {
        meterRegistry.counter(RETRY_EXHAUSTED_COUNTER,
                TAG_SAGA_ID,    sagaId,
                TAG_DCN_ID,     debitCreditNoteId,
                TAG_DCN_NUMBER, documentNumber)
            .increment();
    }
}
```

- [ ] **Step 9: Commit**

```bash
git add src/
git commit -m "feat: add storage, REST client, config, and metrics infrastructure"
```

---

## Task 11: Application use cases + document service + saga command handler

**Files:**
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/ProcessDebitCreditNotePdfUseCase.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/usecase/CompensateDebitCreditNotePdfUseCase.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/service/DebitCreditNotePdfDocumentService.java`
- Create: `src/main/java/com/wpanther/debitcreditnote/pdf/application/service/SagaCommandHandler.java`

- [ ] **Step 1: Create `ProcessDebitCreditNotePdfUseCase.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;

public interface ProcessDebitCreditNotePdfUseCase {
    void handle(KafkaDebitCreditNoteProcessCommand command);
}
```

- [ ] **Step 2: Create `CompensateDebitCreditNotePdfUseCase.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.usecase;

import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;

public interface CompensateDebitCreditNotePdfUseCase {
    void handle(KafkaDebitCreditNoteCompensateCommand command);
}
```

- [ ] **Step 3: Create `DebitCreditNotePdfDocumentService.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging.DebitCreditNotePdfGeneratedEvent;
import com.wpanther.debitcreditnote.pdf.infrastructure.metrics.PdfGenerationMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DebitCreditNotePdfDocumentService {

    private final DebitCreditNotePdfDocumentRepository repository;
    private final PdfEventPort pdfEventPort;
    private final SagaReplyPort sagaReplyPort;
    private final PdfGenerationMetrics pdfGenerationMetrics;

    @Transactional(readOnly = true)
    public Optional<DebitCreditNotePdfDocument> findByDebitCreditNoteId(String debitCreditNoteId) {
        return repository.findByDebitCreditNoteId(debitCreditNoteId);
    }

    @Transactional
    public DebitCreditNotePdfDocument beginGeneration(String debitCreditNoteId, String documentNumber) {
        log.info("Initiating PDF generation for debit/credit note: {}", documentNumber);
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(debitCreditNoteId)
                .documentNumber(documentNumber)
                .build();
        doc.startGeneration();
        return repository.save(doc);
    }

    @Transactional
    public DebitCreditNotePdfDocument replaceAndBeginGeneration(
            UUID existingId, int previousRetryCount, String debitCreditNoteId, String documentNumber) {
        log.info("Replacing document {} and re-starting generation for: {}", existingId, documentNumber);
        repository.deleteById(existingId);
        repository.flush();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId(debitCreditNoteId)
                .documentNumber(documentNumber)
                .build();
        doc.startGeneration();
        doc.incrementRetryCountTo(previousRetryCount + 1);
        return repository.save(doc);
    }

    @Transactional
    public void completeGenerationAndPublish(UUID documentId, String s3Key, String fileUrl,
                                             long fileSize, int previousRetryCount,
                                             KafkaDebitCreditNoteProcessCommand command) {
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markCompleted(s3Key, fileUrl, fileSize);
        doc.markXmlEmbedded();
        applyRetryCount(doc, previousRetryCount);
        doc = repository.save(doc);

        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(doc, command));
        sagaReplyPort.publishSuccess(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                doc.getDocumentUrl(), doc.getFileSize());

        log.info("Completed PDF generation for saga {} debit/credit note {}",
                command.getSagaId(), doc.getDocumentNumber());
    }

    @Transactional
    public void failGenerationAndPublish(UUID documentId, String errorMessage,
                                         int previousRetryCount,
                                         KafkaDebitCreditNoteProcessCommand command) {
        String safeError = errorMessage != null ? errorMessage : "PDF generation failed";
        DebitCreditNotePdfDocument doc = requireDocument(documentId);
        doc.markFailed(safeError);
        applyRetryCount(doc, previousRetryCount);
        repository.save(doc);
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), safeError);
        log.warn("PDF generation failed for saga {} debit/credit note {}: {}",
                command.getSagaId(), doc.getDocumentNumber(), safeError);
    }

    @Transactional
    public void deleteById(UUID documentId) {
        repository.deleteById(documentId);
        repository.flush();
    }

    @Transactional
    public void publishIdempotentSuccess(DebitCreditNotePdfDocument existing,
                                         KafkaDebitCreditNoteProcessCommand command) {
        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(existing, command));
        sagaReplyPort.publishSuccess(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                existing.getDocumentUrl(), existing.getFileSize());
        log.warn("Debit/credit note PDF already generated for saga {} — re-publishing SUCCESS reply",
                command.getSagaId());
    }

    @Transactional
    public void publishRetryExhausted(KafkaDebitCreditNoteProcessCommand command) {
        pdfGenerationMetrics.recordRetryExhausted(
                command.getSagaId(), command.getDocumentId(), command.getDocumentNumber());
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                "Maximum retry attempts exceeded");
        log.error("Max retries exceeded for saga {} document {}", command.getSagaId(), command.getDocumentNumber());
    }

    @Transactional
    public void publishGenerationFailure(KafkaDebitCreditNoteProcessCommand command, String errorMessage) {
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), errorMessage);
    }

    @Transactional
    public void publishCompensated(KafkaDebitCreditNoteCompensateCommand command) {
        sagaReplyPort.publishCompensated(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId());
    }

    @Transactional
    public void publishCompensationFailure(KafkaDebitCreditNoteCompensateCommand command, String error) {
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), error);
    }

    private DebitCreditNotePdfDocument requireDocument(UUID documentId) {
        return repository.findById(documentId)
                .orElseThrow(() -> new IllegalStateException(
                        "Expected debit/credit note PDF document is absent: " + documentId));
    }

    private void applyRetryCount(DebitCreditNotePdfDocument doc, int previousRetryCount) {
        if (previousRetryCount < 0) return;
        doc.incrementRetryCountTo(previousRetryCount + 1);
    }

    private DebitCreditNotePdfGeneratedEvent buildGeneratedEvent(DebitCreditNotePdfDocument doc,
                                                                  KafkaDebitCreditNoteProcessCommand command) {
        return new DebitCreditNotePdfGeneratedEvent(
                command.getSagaId(), command.getDocumentId(), doc.getDocumentNumber(),
                doc.getDocumentUrl(), doc.getFileSize(), doc.isXmlEmbedded(), command.getCorrelationId());
    }
}
```

- [ ] **Step 4: Create `SagaCommandHandler.java`**

```java
package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfStoragePort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.debitcreditnote.pdf.application.usecase.CompensateDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.application.usecase.ProcessDebitCreditNotePdfUseCase;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.service.DebitCreditNotePdfGenerationService;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;

import java.util.Optional;

@Service
@Slf4j
public class SagaCommandHandler implements ProcessDebitCreditNotePdfUseCase, CompensateDebitCreditNotePdfUseCase {

    private static final String MDC_SAGA_ID         = "sagaId";
    private static final String MDC_CORRELATION_ID  = "correlationId";
    private static final String MDC_DOCUMENT_NUMBER = "documentNumber";
    private static final String MDC_DOCUMENT_ID     = "documentId";

    private final DebitCreditNotePdfDocumentService pdfDocumentService;
    private final DebitCreditNotePdfGenerationService pdfGenerationService;
    private final PdfStoragePort pdfStoragePort;
    private final SagaReplyPort sagaReplyPort;
    private final SignedXmlFetchPort signedXmlFetchPort;
    private final int maxRetries;

    public SagaCommandHandler(DebitCreditNotePdfDocumentService pdfDocumentService,
                              DebitCreditNotePdfGenerationService pdfGenerationService,
                              PdfStoragePort pdfStoragePort,
                              SagaReplyPort sagaReplyPort,
                              SignedXmlFetchPort signedXmlFetchPort,
                              @Value("${app.pdf.generation.max-retries:3}") int maxRetries) {
        this.pdfDocumentService  = pdfDocumentService;
        this.pdfGenerationService = pdfGenerationService;
        this.pdfStoragePort      = pdfStoragePort;
        this.sagaReplyPort       = sagaReplyPort;
        this.signedXmlFetchPort  = signedXmlFetchPort;
        this.maxRetries          = maxRetries;
    }

    @Override
    public void handle(KafkaDebitCreditNoteProcessCommand command) {
        MDC.put(MDC_SAGA_ID,         command.getSagaId());
        MDC.put(MDC_CORRELATION_ID,  command.getCorrelationId());
        MDC.put(MDC_DOCUMENT_NUMBER, command.getDocumentNumber());
        MDC.put(MDC_DOCUMENT_ID,     command.getDocumentId());
        try {
            log.info("Handling ProcessCommand for saga {} document {}",
                    command.getSagaId(), command.getDocumentNumber());
            try {
                String signedXmlUrl = command.getSignedXmlUrl();
                String documentId   = command.getDocumentId();
                String documentNum  = command.getDocumentNumber();

                if (signedXmlUrl == null || signedXmlUrl.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(command, "signedXmlUrl is null or blank");
                    return;
                }
                if (documentId == null || documentId.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(command, "documentId is null or blank");
                    return;
                }
                if (documentNum == null || documentNum.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(command, "documentNumber is null or blank");
                    return;
                }

                Optional<DebitCreditNotePdfDocument> existing =
                        pdfDocumentService.findByDebitCreditNoteId(documentId);

                if (existing.isPresent() && existing.get().isCompleted()) {
                    pdfDocumentService.publishIdempotentSuccess(existing.get(), command);
                    return;
                }

                int previousRetryCount = existing.map(DebitCreditNotePdfDocument::getRetryCount).orElse(-1);

                if (existing.isPresent() && existing.get().isMaxRetriesExceeded(maxRetries)) {
                    pdfDocumentService.publishRetryExhausted(command);
                    return;
                }

                DebitCreditNotePdfDocument document;
                if (existing.isPresent()) {
                    document = pdfDocumentService.replaceAndBeginGeneration(
                            existing.get().getId(), previousRetryCount, documentId, documentNum);
                } else {
                    document = pdfDocumentService.beginGeneration(documentId, documentNum);
                }

                String s3Key = null;
                try {
                    String signedXml = signedXmlFetchPort.fetch(signedXmlUrl);
                    byte[] pdfBytes  = pdfGenerationService.generatePdf(documentNum, signedXml);
                    s3Key = pdfStoragePort.store(documentNum, pdfBytes);
                    String fileUrl   = pdfStoragePort.resolveUrl(s3Key);

                    pdfDocumentService.completeGenerationAndPublish(
                            document.getId(), s3Key, fileUrl, pdfBytes.length, previousRetryCount, command);

                } catch (CallNotPermittedException e) {
                    log.warn("Circuit breaker OPEN for saga {} document {}: {}",
                            command.getSagaId(), documentNum, e.getMessage());
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), "Circuit breaker open: " + e.getMessage(),
                            previousRetryCount, command);

                } catch (RestClientException e) {
                    log.warn("HTTP error fetching signed XML for saga {} document {}: {}",
                            command.getSagaId(), documentNum, e.getMessage());
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), "HTTP error fetching signed XML: " + describeThrowable(e),
                            previousRetryCount, command);

                } catch (Exception e) {
                    if (s3Key != null) {
                        try { pdfStoragePort.delete(s3Key); }
                        catch (Exception del) {
                            log.error("[ORPHAN_PDF] s3Key={} saga={} error={}", s3Key, command.getSagaId(),
                                    describeThrowable(del));
                        }
                    }
                    log.error("PDF generation failed for saga {} document {}: {}",
                            command.getSagaId(), documentNum, e.getMessage(), e);
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), describeThrowable(e), previousRetryCount, command);
                }

            } catch (OptimisticLockingFailureException e) {
                log.warn("Concurrent modification for saga {}: {}", command.getSagaId(), e.getMessage());
                pdfDocumentService.publishGenerationFailure(command, "Concurrent modification: " + e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error for saga {}: {}", command.getSagaId(), e.getMessage(), e);
                pdfDocumentService.publishGenerationFailure(command, describeThrowable(e));
            }
        } finally {
            MDC.clear();
        }
    }

    @Override
    public void handle(KafkaDebitCreditNoteCompensateCommand command) {
        MDC.put(MDC_SAGA_ID,        command.getSagaId());
        MDC.put(MDC_CORRELATION_ID,  command.getCorrelationId());
        MDC.put(MDC_DOCUMENT_ID,     command.getDocumentId());
        try {
            log.info("Handling compensation for saga {} document {}",
                    command.getSagaId(), command.getDocumentId());
            try {
                Optional<DebitCreditNotePdfDocument> existing =
                        pdfDocumentService.findByDebitCreditNoteId(command.getDocumentId());
                if (existing.isPresent()) {
                    DebitCreditNotePdfDocument doc = existing.get();
                    pdfDocumentService.deleteById(doc.getId());
                    if (doc.getDocumentPath() != null) {
                        try { pdfStoragePort.delete(doc.getDocumentPath()); }
                        catch (Exception e) {
                            log.warn("Failed to delete PDF from MinIO for saga {} key {}: {}",
                                    command.getSagaId(), doc.getDocumentPath(), e.getMessage());
                        }
                    }
                    log.info("Compensated document {} for saga {}", doc.getId(), command.getSagaId());
                } else {
                    log.info("No document for documentId {} — already compensated", command.getDocumentId());
                }
                pdfDocumentService.publishCompensated(command);
            } catch (Exception e) {
                log.error("Failed to compensate for saga {}: {}", command.getSagaId(), e.getMessage(), e);
                pdfDocumentService.publishCompensationFailure(
                        command, "Compensation failed: " + describeThrowable(e));
            }
        } finally {
            MDC.clear();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailure(KafkaDebitCreditNoteProcessCommand command, Throwable cause) {
        try {
            sagaReplyPort.publishFailure(command.getSagaId(), command.getSagaStep(),
                    command.getCorrelationId(),
                    "Message routed to DLQ after retry exhaustion: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of DLQ failure for saga {}", command.getSagaId(), e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishCompensationOrchestrationFailure(KafkaDebitCreditNoteCompensateCommand command,
                                                         Throwable cause) {
        try {
            sagaReplyPort.publishFailure(command.getSagaId(), command.getSagaStep(),
                    command.getCorrelationId(),
                    "Compensation DLQ after retry exhaustion: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of compensation DLQ failure for saga {}", command.getSagaId(), e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailureForUnparsedMessage(
            String sagaId, SagaStep sagaStep, String correlationId, Throwable cause) {
        try {
            sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId,
                    "Message routed to DLQ after deserialization failure: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of DLQ deserialization failure for saga {} — orchestrator must timeout",
                    sagaId, e);
        }
    }

    private String describeThrowable(Throwable t) {
        if (t == null) return "unknown error";
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg != null ? ": " + msg : "");
    }
}
```

- [ ] **Step 5: Commit**

```bash
git add src/
git commit -m "feat: add application use cases, document service, and saga command handler"
```

---

## Task 12: Resources — `application.yml`, Flyway migration, XSL template, fonts, test resources

**Files:**
- Create: `src/main/resources/application.yml`
- Create: `src/main/resources/db/migration/V1__create_debit_credit_note_pdf_tables.sql`
- Create: `src/main/resources/xsl/debitcreditnote-direct.xsl`
- Copy:   `src/main/resources/fop/fop.xconf` (from taxinvoice)
- Copy:   `src/main/resources/icc/sRGB.icc` (from taxinvoice)
- Copy:   `src/main/resources/fonts/` (all 6 TTF files from taxinvoice)
- Create: `src/test/resources/fop/fop.xconf`
- Create: `src/test/resources/xml/preview-debitcreditnote.xml`

- [ ] **Step 1: Create `application.yml`**

```yaml
server:
  port: 8097

spring:
  application:
    name: debitcreditnote-pdf-generation-service

  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:debitcreditnotepdf_db}
    username: ${DB_USERNAME:postgres}
    password: ${DB_PASSWORD:postgres}
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5

  jpa:
    database-platform: org.hibernate.dialect.PostgreSQLDialect
    hibernate:
      ddl-auto: validate
    show-sql: false

  flyway:
    enabled: true
    locations: classpath:db/migration

  jackson:
    serialization:
      write-dates-as-timestamps: false

camel:
  springboot:
    name: debitcreditnote-pdf-generation-camel
    main-run-controller: true
  dataformat:
    jackson:
      auto-discover-object-mapper: true

app:
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}
    consumer:
      command-group-id: ${KAFKA_COMMAND_GROUP_ID:debitcreditnote-pdf-generation-command}
      compensation-group-id: ${KAFKA_COMPENSATION_GROUP_ID:debitcreditnote-pdf-generation-compensation}
      break-on-first-error: ${KAFKA_BREAK_ON_FIRST_ERROR:true}
      max-poll-records: ${KAFKA_MAX_POLL_RECORDS:100}
      consumers-count: ${KAFKA_CONSUMERS_COUNT:3}
    topics:
      saga-command-debit-credit-note-pdf: saga.command.debit-credit-note-pdf
      saga-compensation-debit-credit-note-pdf: saga.compensation.debit-credit-note-pdf
      pdf-generated-debit-credit-note: pdf.generated.debit-credit-note
      dlq: pdf.generation.debit-credit-note.dlq
  minio:
    endpoint: ${MINIO_ENDPOINT:http://localhost:9000}
    access-key: ${MINIO_ACCESS_KEY:minioadmin}
    secret-key: ${MINIO_SECRET_KEY:minioadmin}
    bucket-name: ${MINIO_BUCKET_NAME:debitcreditnotes}
    region: ${MINIO_REGION:us-east-1}
    base-url: ${MINIO_BASE_URL:http://localhost:9000/debitcreditnotes}
    path-style-access: ${MINIO_PATH_STYLE_ACCESS:true}
    cleanup:
      enabled: ${MINIO_CLEANUP_ENABLED:false}
      cron: ${MINIO_CLEANUP_CRON:0 0 2 * * ?}
  pdf:
    icc-profile-path: ${PDF_ICC_PROFILE_PATH:icc/sRGB.icc}
    generation:
      max-retries: ${PDF_GENERATION_MAX_RETRIES:3}
      max-concurrent-renders: ${PDF_MAX_CONCURRENT_RENDERS:3}
      max-pdf-size-bytes: ${PDF_MAX_SIZE_BYTES:52428800}
  debitcreditnote:
    default-vat-rate: ${DEBITCREDITNOTE_DEFAULT_VAT_RATE:7}
  rest-client:
    connect-timeout: ${REST_CLIENT_CONNECT_TIMEOUT:5000}
    read-timeout: ${REST_CLIENT_READ_TIMEOUT:10000}
    allowed-hosts: ${REST_CLIENT_ALLOWED_HOSTS:localhost}
  fonts:
    health-check:
      enabled: ${FONT_HEALTH_CHECK_ENABLED:true}
      fail-on-error: ${FONT_HEALTH_CHECK_FAIL_ON_ERROR:true}

resilience4j:
  circuitbreaker:
    instances:
      minio:
        sliding-window-size: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        register-health-indicator: true
      signedXmlFetch:
        sliding-window-size: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 60s
        permitted-number-of-calls-in-half-open-state: 3
        slow-call-rate-threshold: 50
        slow-call-duration-threshold: 3s

eureka:
  client:
    service-url:
      defaultZone: ${EUREKA_URL:http://localhost:8761/eureka/}
    register-with-eureka: true
    fetch-registry: true
  instance:
    prefer-ip-address: true
    instance-id: ${spring.application.name}:${random.value}

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus,camelroutes
  endpoint:
    health:
      show-details: when-authorized
  metrics:
    tags:
      application: ${spring.application.name}
  tracing:
    sampling:
      probability: ${TRACING_SAMPLING_PROBABILITY:1.0}
  otlp:
    tracing:
      endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4318/v1/traces}

logging:
  level:
    root: INFO
    com.wpanther.debitcreditnote.pdf: INFO
    org.apache.camel: INFO
    org.apache.camel.component.kafka: DEBUG
    org.apache.fop: INFO
    org.apache.pdfbox: INFO
```

- [ ] **Step 2: Create `V1__create_debit_credit_note_pdf_tables.sql`**

```sql
CREATE TABLE debit_credit_note_pdf_documents (
    id                   UUID PRIMARY KEY,
    debit_credit_note_id VARCHAR(100) NOT NULL UNIQUE,
    document_number      VARCHAR(50)  NOT NULL,
    document_path        VARCHAR(500),
    document_url         VARCHAR(1000),
    file_size            BIGINT,
    mime_type            VARCHAR(100) NOT NULL DEFAULT 'application/pdf',
    xml_embedded         BOOLEAN      NOT NULL DEFAULT false,
    status               VARCHAR(20)  NOT NULL,
    error_message        TEXT,
    retry_count          INT          NOT NULL DEFAULT 0,
    created_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at         TIMESTAMP
);

CREATE INDEX idx_dcn_pdf_dcn_id    ON debit_credit_note_pdf_documents(debit_credit_note_id);
CREATE INDEX idx_dcn_pdf_doc_number ON debit_credit_note_pdf_documents(document_number);
CREATE INDEX idx_dcn_pdf_status     ON debit_credit_note_pdf_documents(status);

CREATE TABLE outbox_events (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id   VARCHAR(100) NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    payload        TEXT         NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    status         VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    retry_count    INT          NOT NULL DEFAULT 0,
    error_message  VARCHAR(1000),
    topic          VARCHAR(255),
    partition_key  VARCHAR(255),
    headers        TEXT
);

CREATE INDEX idx_outbox_status    ON outbox_events(status);
CREATE INDEX idx_outbox_created   ON outbox_events(created_at);
CREATE INDEX idx_outbox_aggregate ON outbox_events(aggregate_id, aggregate_type);
CREATE INDEX idx_outbox_compound  ON outbox_events(status, created_at);
```

- [ ] **Step 3: Create `debitcreditnote-direct.xsl`**

Copy `taxinvoice-pdf-generation-service/src/main/resources/xsl/taxinvoice-direct.xsl` verbatim, applying the namespace substitution table from the File Map section:

- Change `xmlns:rsm="urn:etda:uncefact:data:standard:TaxInvoice_CrossIndustryInvoice:2"` to `xmlns:rsm="urn:etda:uncefact:data:standard:DebitCreditNote_CrossIndustryInvoice:2"`
- Change `xmlns:ram="urn:etda:uncefact:data:standard:TaxInvoice_ReusableAggregateBusinessInformationEntity:2"` to `xmlns:ram="urn:etda:uncefact:data:standard:DebitCreditNote_ReusableAggregateBusinessInformationEntity:2"`
- Change `<xsl:template match="/rsm:TaxInvoice_CrossIndustryInvoice">` to `<xsl:template match="/rsm:DebitCreditNote_CrossIndustryInvoice">`
- Change the static document title string (e.g., `ใบเสร็จรับเงิน/ใบกำกับภาษี`) to `<xsl:value-of select="$doc/ram:Name"/>` (dynamic from XML)
- Change the page header label (e.g., `e-Tax Tax Invoice / ใบเสร็จรับเงิน/ใบกำกับภาษีอิเล็กทรอนิกส์`) to `e-Tax Debit/Credit Note / ใบเพิ่มหนี้/ใบลดหนี้อิเล็กทรอนิกส์`

Additionally, add two new sections (wrapped in `<xsl:if test="...">` to handle optional fields):

**In the document header block** (after document ID and issue date rows), add:

```xml
<xsl:if test="$doc/ram:Purpose">
  <fo:table-row>
    <fo:table-cell>
      <fo:block font-family="{$font-family}" font-size="{$font-size}">เหตุผล (Purpose)</fo:block>
    </fo:table-cell>
    <fo:table-cell>
      <fo:block font-family="{$font-family}" font-size="{$font-size}">
        <xsl:value-of select="$doc/ram:Purpose"/>
      </fo:block>
    </fo:table-cell>
  </fo:table-row>
</xsl:if>
<xsl:if test="$doc/ram:PurposeCode">
  <fo:table-row>
    <fo:table-cell>
      <fo:block font-family="{$font-family}" font-size="{$font-size}">รหัสเหตุผล (Purpose Code)</fo:block>
    </fo:table-cell>
    <fo:table-cell>
      <fo:block font-family="{$font-family}" font-size="{$font-size}">
        <xsl:value-of select="$doc/ram:PurposeCode"/>
      </fo:block>
    </fo:table-cell>
  </fo:table-row>
</xsl:if>
```

**In the monetary summary table** (after GrandTotalAmount row), add:

```xml
<xsl:if test="$summation/ram:OriginalInformationAmount">
  <fo:table-row>
    <fo:table-cell><fo:block font-family="{$font-family}" font-size="{$font-size}">ยอดเงินตามเอกสารเดิม</fo:block></fo:table-cell>
    <fo:table-cell text-align="right"><fo:block font-family="{$font-family}" font-size="{$font-size}">
      <xsl:value-of select="format-number($summation/ram:OriginalInformationAmount, '#,##0.00')"/>
    </fo:block></fo:table-cell>
  </fo:table-row>
</xsl:if>
<xsl:if test="$summation/ram:DifferenceInformationAmount">
  <fo:table-row>
    <fo:table-cell><fo:block font-family="{$font-family}" font-size="{$font-size}">ผลต่าง (Difference)</fo:block></fo:table-cell>
    <fo:table-cell text-align="right"><fo:block font-family="{$font-family}" font-size="{$font-size}">
      <xsl:value-of select="format-number($summation/ram:DifferenceInformationAmount, '#,##0.00')"/>
    </fo:block></fo:table-cell>
  </fo:table-row>
</xsl:if>
```

- [ ] **Step 4: Copy binary resources from taxinvoice service**

```bash
TAXINVOICE_SRC=../taxinvoice-pdf-generation-service/src/main/resources

# FOP config
mkdir -p src/main/resources/fop
cp $TAXINVOICE_SRC/fop/fop.xconf src/main/resources/fop/

# ICC profile
mkdir -p src/main/resources/icc
cp $TAXINVOICE_SRC/icc/sRGB.icc src/main/resources/icc/

# Thai fonts
mkdir -p src/main/resources/fonts
cp $TAXINVOICE_SRC/fonts/*.ttf src/main/resources/fonts/
cp $TAXINVOICE_SRC/fonts/README.md src/main/resources/fonts/
```

- [ ] **Step 5: Create test `fop/fop.xconf`**

Copy `taxinvoice-pdf-generation-service/src/test/resources/fop/fop.xconf` verbatim into `src/test/resources/fop/fop.xconf` (the test FOP config disables PDF/A mode and uses auto-detect fonts so no Thai font files are needed during tests).

- [ ] **Step 6: Create test XML `preview-debitcreditnote.xml`**

Copy the content of `Example_CreditNote_2p1_v1.xml` (from `etax/teda/src/main/resources/e-tax-invoice-receipt-v2.1/ETDA/ExampleFile/Example_CreditNote_2p1_v1.xml`) to `src/test/resources/xml/preview-debitcreditnote.xml`.

- [ ] **Step 7: Verify full compile**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`

- [ ] **Step 8: Commit**

```bash
git add src/
git commit -m "feat: add application.yml, Flyway migration, XSL template, and static resources"
```

---

## Task 13: Full test suite

**Files:** All test classes listed in the File Map.

- [ ] **Step 1: Run all existing tests to confirm baseline**

```bash
mvn test -Dspring.profiles.active=test 2>&1 | tail -15
```

Expected: All previously written tests pass (domain model, exception, constants, persistence, command mapper).

- [ ] **Step 2: Write `SagaCommandHandlerTest.java`**

Mirror `taxinvoice-pdf-generation-service/src/test/java/.../application/service/SagaCommandHandlerTest.java`, substituting all `TaxInvoice`/`taxInvoice` identifiers with `DebitCreditNote`/`debitCreditNote`, and `KafkaTaxInvoiceProcessCommand`/`KafkaTaxInvoiceCompensateCommand` with the debit/credit note equivalents.

Key test scenarios (identical to taxinvoice):
1. `handle_processCommand_success` — new document, generate, upload, complete
2. `handle_processCommand_idempotentSuccess` — document already COMPLETED
3. `handle_processCommand_maxRetriesExceeded` — `retryCount >= maxRetries`
4. `handle_processCommand_generationFailure` — `pdfGenerationService` throws
5. `handle_compensateCommand_success` — document exists, deleted, MinIO cleaned
6. `handle_compensateCommand_idempotent` — no document found
7. `handle_compensateCommand_failure` — delete throws exception

```java
package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfStoragePort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.service.DebitCreditNotePdfGenerationService;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SagaCommandHandlerTest {

    @Mock private DebitCreditNotePdfDocumentService pdfDocumentService;
    @Mock private DebitCreditNotePdfGenerationService pdfGenerationService;
    @Mock private PdfStoragePort pdfStoragePort;
    @Mock private SagaReplyPort sagaReplyPort;
    @Mock private SignedXmlFetchPort signedXmlFetchPort;

    private SagaCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new SagaCommandHandler(
                pdfDocumentService, pdfGenerationService,
                pdfStoragePort, sagaReplyPort, signedXmlFetchPort, 3);
    }

    private KafkaDebitCreditNoteProcessCommand processCommand() {
        return new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", "DCN-2024-001", "http://storage/signed.xml");
    }

    private KafkaDebitCreditNoteCompensateCommand compensateCommand() {
        return new KafkaDebitCreditNoteCompensateCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1", "dcn-001");
    }

    private DebitCreditNotePdfDocument generatingDoc() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001").build();
        doc.startGeneration();
        return doc;
    }

    @Test
    void handle_processCommand_success() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        when(signedXmlFetchPort.fetch("http://storage/signed.xml")).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf("DCN-2024-001", "<xml/>")).thenReturn(new byte[100]);
        when(pdfStoragePort.store("DCN-2024-001", new byte[100])).thenReturn("2024/01/15/test.pdf");
        when(pdfStoragePort.resolveUrl("2024/01/15/test.pdf")).thenReturn("http://minio/test.pdf");

        handler.handle(processCommand());

        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(doc.getId()), eq("2024/01/15/test.pdf"), eq("http://minio/test.pdf"),
                eq(100L), eq(-1), any());
    }

    @Test
    void handle_processCommand_idempotentSuccess() {
        DebitCreditNotePdfDocument completedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.COMPLETED).documentUrl("http://minio/existing.pdf")
                .fileSize(9999L).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001"))
                .thenReturn(Optional.of(completedDoc));

        handler.handle(processCommand());

        verify(pdfDocumentService).publishIdempotentSuccess(eq(completedDoc), any());
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_processCommand_maxRetriesExceeded() {
        DebitCreditNotePdfDocument failedDoc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.FAILED).retryCount(3).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001"))
                .thenReturn(Optional.of(failedDoc));

        handler.handle(processCommand());

        verify(pdfDocumentService).publishRetryExhausted(any());
        verify(pdfGenerationService, never()).generatePdf(anyString(), anyString());
    }

    @Test
    void handle_processCommand_generationFailure() throws Exception {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        DebitCreditNotePdfDocument doc = generatingDoc();
        when(pdfDocumentService.beginGeneration("dcn-001", "DCN-2024-001")).thenReturn(doc);
        when(signedXmlFetchPort.fetch(anyString())).thenReturn("<xml/>");
        when(pdfGenerationService.generatePdf(anyString(), anyString()))
                .thenThrow(new RuntimeException("FOP failed"));

        handler.handle(processCommand());

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(doc.getId()), contains("FOP failed"), eq(-1), any());
    }

    @Test
    void handle_compensateCommand_success() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .documentPath("2024/01/15/test.pdf").status(GenerationStatus.COMPLETED).build();
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.of(doc));

        handler.handle(compensateCommand());

        verify(pdfDocumentService).deleteById(doc.getId());
        verify(pdfStoragePort).delete("2024/01/15/test.pdf");
        verify(pdfDocumentService).publishCompensated(any());
    }

    @Test
    void handle_compensateCommand_idempotent() {
        when(pdfDocumentService.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());

        handler.handle(compensateCommand());

        verify(pdfDocumentService, never()).deleteById(any());
        verify(pdfDocumentService).publishCompensated(any());
    }

    @Test
    void handle_processCommand_nullSignedXmlUrl() {
        KafkaDebitCreditNoteProcessCommand cmd = new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", "DCN-2024-001", null);
        when(pdfDocumentService.findByDebitCreditNoteId(anyString())).thenReturn(Optional.empty());

        handler.handle(cmd);

        verify(pdfDocumentService).publishGenerationFailure(any(), contains("signedXmlUrl"));
    }
}
```

- [ ] **Step 3: Run `SagaCommandHandlerTest`**

```bash
mvn test -Dtest=SagaCommandHandlerTest -Dspring.profiles.active=test 2>&1 | tail -10
```

Expected: `Tests run: 7, Failures: 0, Errors: 0`

- [ ] **Step 4: Write `DebitCreditNotePdfDocumentServiceTest.java`**

Mirror `taxinvoice-pdf-generation-service/.../TaxInvoicePdfDocumentServiceTest.java`, changing all identifiers to `DebitCreditNote` equivalents. Key scenarios: `findByDebitCreditNoteId`, `beginGeneration`, `replaceAndBeginGeneration`, `completeGenerationAndPublish`, `failGenerationAndPublish`, `publishIdempotentSuccess`, `publishRetryExhausted`.

Use `@ExtendWith(MockitoExtension.class)` with mocks for `DebitCreditNotePdfDocumentRepository`, `PdfEventPort`, `SagaReplyPort`, `PdfGenerationMetrics`. The test confirms all `@Transactional` methods call the correct repository and port methods.

```java
package com.wpanther.debitcreditnote.pdf.application.service;

import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.domain.model.DebitCreditNotePdfDocument;
import com.wpanther.debitcreditnote.pdf.domain.model.GenerationStatus;
import com.wpanther.debitcreditnote.pdf.domain.repository.DebitCreditNotePdfDocumentRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.in.kafka.KafkaDebitCreditNoteProcessCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.metrics.PdfGenerationMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DebitCreditNotePdfDocumentServiceTest {

    @Mock private DebitCreditNotePdfDocumentRepository repository;
    @Mock private PdfEventPort pdfEventPort;
    @Mock private SagaReplyPort sagaReplyPort;
    @Mock private PdfGenerationMetrics metrics;
    @InjectMocks private DebitCreditNotePdfDocumentService service;

    private KafkaDebitCreditNoteProcessCommand command() {
        return new KafkaDebitCreditNoteProcessCommand(
                "saga-1", SagaStep.GENERATE_DEBIT_CREDIT_NOTE_PDF, "corr-1",
                "dcn-001", "DCN-2024-001", "http://storage/signed.xml");
    }

    @Test
    void findByDebitCreditNoteId_delegatesToRepository() {
        when(repository.findByDebitCreditNoteId("dcn-001")).thenReturn(Optional.empty());
        assertThat(service.findByDebitCreditNoteId("dcn-001")).isEmpty();
    }

    @Test
    void beginGeneration_savesGeneratingDocument() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.save(any())).thenReturn(doc);

        DebitCreditNotePdfDocument result = service.beginGeneration("dcn-001", "DCN-2024-001");

        assertThat(result.getStatus()).isEqualTo(GenerationStatus.GENERATING);
    }

    @Test
    void completeGenerationAndPublish_marksCompletedAndPublishes() {
        UUID docId = UUID.randomUUID();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(docId).debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.findById(docId)).thenReturn(Optional.of(doc));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.completeGenerationAndPublish(
                docId, "2024/01/15/test.pdf", "http://minio/test.pdf", 12345L, -1, command());

        verify(pdfEventPort).publishPdfGenerated(any());
        verify(sagaReplyPort).publishSuccess(eq("saga-1"), any(), eq("corr-1"),
                eq("http://minio/test.pdf"), eq(12345L));
    }

    @Test
    void failGenerationAndPublish_marksFailedAndPublishes() {
        UUID docId = UUID.randomUUID();
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(docId).debitCreditNoteId("dcn-001").documentNumber("DCN-2024-001")
                .status(GenerationStatus.GENERATING).build();
        when(repository.findById(docId)).thenReturn(Optional.of(doc));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.failGenerationAndPublish(docId, "FOP crash", -1, command());

        verify(sagaReplyPort).publishFailure(eq("saga-1"), any(), eq("corr-1"), eq("FOP crash"));
    }

    @Test
    void publishRetryExhausted_recordsMetricAndPublishesFailure() {
        service.publishRetryExhausted(command());

        verify(metrics).recordRetryExhausted("saga-1", "dcn-001", "DCN-2024-001");
        verify(sagaReplyPort).publishFailure(eq("saga-1"), any(), eq("corr-1"),
                contains("Maximum retry attempts exceeded"));
    }
}
```

- [ ] **Step 5: Run `DebitCreditNotePdfDocumentServiceTest`**

```bash
mvn test -Dtest=DebitCreditNotePdfDocumentServiceTest -Dspring.profiles.active=test 2>&1 | tail -5
```

Expected: `Tests run: 5, Failures: 0, Errors: 0`

- [ ] **Step 6: Run the full test suite**

```bash
mvn test -Dspring.profiles.active=test 2>&1 | tail -15
```

Expected: All tests pass. If any test fails due to Spring context not loading (missing beans), check that `application-test.yml` has `eureka.client.enabled: false` and `management.tracing.enabled: false`.

- [ ] **Step 7: Commit**

```bash
git add src/
git commit -m "feat: add full test suite — SagaCommandHandlerTest and DebitCreditNotePdfDocumentServiceTest"
```

---

## Task 14: Build verification + remaining tests

- [ ] **Step 1: Full build with tests**

```bash
mvn clean verify -Dspring.profiles.active=test 2>&1 | tail -20
```

Expected: `BUILD SUCCESS`. If JaCoCo reports below 90% line coverage on any package, add the missing test cases from the test class list in the File Map (e.g., `ThaiAmountWordsConverterTest`, `PdfA3ConverterTest`, `FopDebitCreditNotePdfGeneratorTest`, etc.).

- [ ] **Step 2: Write `ThaiAmountWordsConverterTest.java`** if coverage requires it

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.pdf;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.*;

class ThaiAmountWordsConverterTest {

    @Test void zero()             { assertThat(ThaiAmountWordsConverter.toWords(BigDecimal.ZERO)).isEqualTo("ศูนย์บาทถ้วน"); }
    @Test void oneHundred()       { assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("100"))).isEqualTo("หนึ่งร้อยบาทถ้วน"); }
    @Test void withSatang()       { assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("1.50"))).contains("สตางค์"); }
    @Test void negativeThrows()   { assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(new BigDecimal("-1"))).isInstanceOf(IllegalArgumentException.class); }
    @Test void nullThrows()       { assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(null)).isInstanceOf(IllegalArgumentException.class); }
    @Test void oneMillion()       { assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("1000000"))).contains("ล้าน"); }
    @Test void twentyOne()        { assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("21"))).contains("ยี่สิบ"); }
    @Test void elevenContainsEt() { assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("11"))).contains("เอ็ด"); }
}
```

- [ ] **Step 3: Write `PdfA3ConverterTest.java`** if coverage requires it

Mirror `taxinvoice-pdf-generation-service/.../PdfA3ConverterTest.java` with the package change. Key tests: constructor with invalid ICC path throws, `PdfConversionException` constructors, `convertToPdfA3` with null/empty PDF throws.

- [ ] **Step 4: Run coverage check**

```bash
mvn verify -Dspring.profiles.active=test 2>&1 | grep -E "FAILED|WARN.*Missed|BUILD"
```

Expected: `BUILD SUCCESS` with no coverage failures.

- [ ] **Step 5: Final commit**

```bash
git add src/
git commit -m "feat: complete debitcreditnote-pdf-generation-service — all tests passing"
```

---

## Task 15: Remaining unit tests for full coverage

**Files:**
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/domain/model/DebitCreditNotePdfDocumentTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/CamelRouteConfigTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/EventPublisherTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisherTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/messaging/KafkaCommandMapperTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/pdf/FopDebitCreditNotePdfGeneratorTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/storage/MinioStorageAdapterTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcherTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/MinioCleanupServiceTest.java`
- Create: `src/test/java/com/wpanther/debitcreditnote/pdf/infrastructure/config/FontHealthCheckTest.java`

- [ ] **Step 1: Write `DebitCreditNotePdfDocumentTest.java`**

```java
package com.wpanther.debitcreditnote.pdf.domain.model;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

class DebitCreditNotePdfDocumentTest {

    @Test
    void startGeneration_transitionsPendingToGenerating() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .status(GenerationStatus.PENDING).build();
        doc.startGeneration();
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.GENERATING);
    }

    @Test
    void startGeneration_whenNotPending_throws() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .status(GenerationStatus.COMPLETED).build();
        assertThatThrownBy(doc::startGeneration).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void markCompleted_transitionsGeneratingToCompleted() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .status(GenerationStatus.GENERATING).build();
        doc.markCompleted("path/abc.pdf", "http://example.com/abc.pdf", 12345L);
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
        assertThat(doc.getDocumentPath()).isEqualTo("path/abc.pdf");
        assertThat(doc.getDocumentUrl()).isEqualTo("http://example.com/abc.pdf");
        assertThat(doc.getFileSize()).isEqualTo(12345L);
    }

    @Test
    void markCompleted_whenNotGenerating_throws() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .status(GenerationStatus.PENDING).build();
        assertThatThrownBy(() -> doc.markCompleted("p", "u", 1L))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void markFailed_fromAnyState_transitionsToFailed() {
        for (GenerationStatus s : GenerationStatus.values()) {
            DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                    .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                    .status(s).build();
            doc.markFailed("error");
            assertThat(doc.getStatus()).isEqualTo(GenerationStatus.FAILED);
            assertThat(doc.getErrorMessage()).isEqualTo("error");
        }
    }

    @Test
    void incrementRetryCountTo_monotonic() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .retryCount(1).build();
        doc.incrementRetryCountTo(3);
        assertThat(doc.getRetryCount()).isEqualTo(3);
    }

    @Test
    void incrementRetryCountTo_decreasing_throws() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .retryCount(3).build();
        assertThatThrownBy(() -> doc.incrementRetryCountTo(1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isMaxRetriesExceeded_trueWhenEqual() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .retryCount(3).build();
        assertThat(doc.isMaxRetriesExceeded(3)).isTrue();
    }

    @Test
    void isMaxRetriesExceeded_falseWhenBelow() {
        DebitCreditNotePdfDocument doc = DebitCreditNotePdfDocument.builder()
                .id(UUID.randomUUID()).debitCreditNoteId("dcn-1").documentNumber("DCN-001")
                .retryCount(2).build();
        assertThat(doc.isMaxRetriesExceeded(3)).isFalse();
    }
}
```

- [ ] **Step 2: Write `CamelRouteConfigTest.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.messaging;

import com.wpanther.debitcreditnote.pdf.application.service.SagaCommandHandler;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.messaging.dto.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.messaging.dto.KafkaDebitCreditNoteProcessCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.repository.JpaDebitCreditNotePdfDocumentRepository;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class CamelRouteConfigTest {

    @Autowired
    private ProducerTemplate producerTemplate;

    @EndpointInject("mock:outbox")
    private MockEndpoint outboxEndpoint;

    @MockBean
    private SagaCommandHandler sagaCommandHandler;

    @MockBean
    private JpaDebitCreditNotePdfDocumentRepository repository;

    @Test
    void processCommand_deserializesCorrectly() throws Exception {
        KafkaDebitCreditNoteProcessCommand cmd = new KafkaDebitCreditNoteProcessCommand(
                UUID.randomUUID(), "2024-01-01T00:00:00Z", "saga.command.debit-credit-note-pdf",
                1, UUID.randomUUID(), "generate-debit-credit-note-pdf",
                UUID.randomUUID(), UUID.randomUUID(), "DCN-001",
                "http://example.com/xml");

        doNothing().when(sagaCommandHandler).handleProcessCommand(any());

        producerTemplate.sendBody("direct:processCommand", cmd);

        outboxEndpoint.expectedMinimumMessageCount(1);
        outboxEndpoint.assertIsSatisfied();
    }

    @Test
    void compensateCommand_deserializesCorrectly() throws Exception {
        KafkaDebitCreditNoteCompensateCommand cmd = new KafkaDebitCreditNoteCompensateCommand(
                UUID.randomUUID(), "2024-01-01T00:00:00Z", "saga.compensation.debit-credit-note-pdf",
                1, UUID.randomUUID(), "generate-debit-credit-note-pdf",
                UUID.randomUUID(), UUID.randomUUID());

        doNothing().when(sagaCommandHandler).handleCompensationCommand(any());

        producerTemplate.sendBody("direct:compensateCommand", cmd);

        outboxEndpoint.expectedMinimumMessageCount(1);
        outboxEndpoint.assertIsSatisfied();
    }
}
```

- [ ] **Step 3: Write `EventPublisherTest.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.wpanther.debitcreditnote.pdf.application.port.out.PdfEventPort;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.OutboxEventEntity;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.SpringDataOutboxRepository;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.messaging.dto.DebitCreditNotePdfGeneratedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class EventPublisherTest {

    @Autowired
    private PdfEventPort eventPort;

    @MockBean
    private SpringDataOutboxRepository outboxRepository;

    @Test
    void publishPdfGenerated_savesToOutbox() {
        DebitCreditNotePdfGeneratedEvent event = new DebitCreditNotePdfGeneratedEvent(
                UUID.randomUUID(), "pdf.generated.debit-credit-note", 1,
                UUID.randomUUID(), UUID.randomUUID(), "DCN-001",
                "http://example.com/abc.pdf", 12345L, true, UUID.randomUUID());

        when(outboxRepository.save(any(OutboxEventEntity.class))).thenAnswer(i -> i.getArgument(0));

        eventPort.publishPdfGenerated(event);

        verify(outboxRepository).save(any(OutboxEventEntity.class));
    }
}
```

- [ ] **Step 4: Write `SagaReplyPublisherTest.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.messaging;

import com.wpanther.debitcreditnote.pdf.application.port.out.SagaReplyPort;
import com.wpanther.debitcreditnote.pdf.domain.model.SagaReplyStatus;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.OutboxEventEntity;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.out.persistence.SpringDataOutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class SagaReplyPublisherTest {

    @Autowired
    private SagaReplyPort sagaReplyPort;

    @MockBean
    private SpringDataOutboxRepository outboxRepository;

    @Test
    void publishSuccess_savesToOutbox() {
        when(outboxRepository.save(any(OutboxEventEntity.class))).thenAnswer(i -> i.getArgument(0));

        sagaReplyPort.publishSuccess(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "http://example.com/abc.pdf", 12345L);

        verify(outboxRepository).save(argThat(e ->
                e.getEventType().equals("saga.reply.debit-credit-note-pdf") &&
                        e.getPayload().contains("\"status\":\"SUCCESS\"")));
    }

    @Test
    void publishFailure_savesToOutbox() {
        when(outboxRepository.save(any(OutboxEventEntity.class))).thenAnswer(i -> i.getArgument(0));

        sagaReplyPort.publishFailure(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "Generation failed");

        verify(outboxRepository).save(argThat(e ->
                e.getEventType().equals("saga.reply.debit-credit-note-pdf") &&
                        e.getPayload().contains("\"status\":\"FAILURE\"")));
    }

    @Test
    void publishCompensated_savesToOutbox() {
        when(outboxRepository.save(any(OutboxEventEntity.class))).thenAnswer(i -> i.getArgument(0));

        sagaReplyPort.publishCompensated(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        verify(outboxRepository).save(argThat(e ->
                e.getEventType().equals("saga.reply.debit-credit-note-pdf") &&
                        e.getPayload().contains("\"status\":\"COMPENSATED\"")));
    }
}
```

- [ ] **Step 5: Write `KafkaCommandMapperTest.java`**

```java
package com.wpanther.debitcreditnote.pdf.infrastructure.adapter.messaging;

import com.wpanther.debitcreditnote.pdf.application.service.SagaCommandHandler;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.messaging.dto.KafkaDebitCreditNoteCompensateCommand;
import com.wpanther.debitcreditnote.pdf.infrastructure.adapter.messaging.dto.KafkaDebitCreditNoteProcessCommand;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class KafkaCommandMapperTest {

    @Autowired
    private SagaCommandHandler handler;

    @Test
    void toProcessCommand_mapsAllFields() {
        KafkaDebitCreditNoteProcessCommand kafkaCmd = new KafkaDebitCreditNoteProcessCommand(
                UUID.randomUUID(), "2024-01-01T00:00:00Z", "saga.command.debit-credit-note-pdf",
                1, UUID.randomUUID(), "generate-debit-credit-note-pdf",
                UUID.randomUUID(), UUID.randomUUID(), "DCN-001",
                "http://example.com/xml");

        SagaCommandHandler.ProcessCommand cmd = handler.toProcessCommand(kafkaCmd);

        assertThat(cmd.sagaId()).isEqualTo(kafkaCmd.getSagaId());
        assertThat(cmd.correlationId()).isEqualTo(kafkaCmd.getCorrelationId());
        assertThat(cmd.documentId()).isEqualTo(kafkaCmd.getDocumentId());
        assertThat(cmd.documentNumber()).isEqualTo(kafkaCmd.getDocumentNumber());
        assertThat(cmd.signedXmlUrl()).isEqualTo(kafkaCmd.getSignedXmlUrl());
    }

    @Test
    void toCompensateCommand_mapsAllFields() {
        KafkaDebitCreditNoteCompensateCommand kafkaCmd = new KafkaDebitCreditNoteCompensateCommand(
                UUID.randomUUID(), "2024-01-01T00:00:00Z", "saga.compensation.debit-credit-note-pdf",
                1, UUID.randomUUID(), "generate-debit-credit-note-pdf",
                UUID.randomUUID(), UUID.randomUUID());

        SagaCommandHandler.CompensateCommand cmd = handler.toCompensateCommand(kafkaCmd);

        assertThat(cmd.sagaId()).isEqualTo(kafkaCmd.getSagaId());
        assertThat(cmd.correlationId()).isEqualTo(kafkaCmd.getCorrelationId());
        assertThat(cmd.debitCreditNoteId()).isEqualTo(kafkaCmd.getDebitCreditNoteId());
    }
}
```

- [ ] **Step 6: Write `FopDebitCreditNotePdfGeneratorTest.java`**

Mirror `taxinvoice-pdf-generation-service/src/test/java/.../FopTaxInvoicePdfGeneratorTest.java`. Key tests:
- Constructor with invalid FOP config path throws
- Constructor with missing font throws
- Constructor validates max concurrent renders
- Valid XML generates PDF (non-empty byte array)
- Malformed XML throws `PdfGenerationException`
- PDF size limit enforced
- Thread interruption handling
- URI resolution for fonts/images
- Font availability check at startup
- Dynamic title from `ram:Name` renders correctly
- Purpose/PurposeCode renders when present, omitted when absent
- OriginalInformationAmount/DifferenceInformationAmount renders when present

- [ ] **Step 7: Write `MinioStorageAdapterTest.java`**

Mirror `taxinvoice-pdf-generation-service/src/test/java/.../MinioStorageAdapterTest.java`. Key tests:
- Upload generates presigned URL
- Delete removes object
- Thai characters in filename handled correctly
- Filename sanitization removes dangerous characters
- Circuit breaker opens after failures
- Bucket name from environment variable

- [ ] **Step 8: Write `RestTemplateSignedXmlFetcherTest.java`**

Mirror `taxinvoice-pdf-generation-service/src/test/java/.../RestTemplateSignedXmlFetcherTest.java`. Key tests:
- Successful fetch returns XML content
- 404 throws `PdfGenerationException`
- Circuit breaker opens after failures
- Read timeout throws `PdfGenerationException`
- Connection timeout throws `PdfGenerationException`

- [ ] **Step 9: Write `MinioCleanupServiceTest.java`**

Mirror `taxinvoice-pdf-generation-service/src/test/java/.../MinioCleanupServiceTest.java`. Key tests:
- Scheduled task deletes old records
- Configuration via `MINIO_CLEANUP_RETENTION_DAYS`
- Only deletes COMPLETED records older than retention

- [ ] **Step 10: Write `FontHealthCheckTest.java`**

Mirror `taxinvoice-pdf-generation-service/src/test/java/.../FontHealthCheckTest.java`. Key tests:
- Health check passes when fonts available
- Health check fails when font files missing
- Health check disabled when `FONT_HEALTH_CHECK_ENABLED=false`

- [ ] **Step 11: Run all unit tests**

```bash
mvn test -Dspring.profiles.active=test 2>&1 | tail -20
```

Expected: All tests pass. JaCoCo coverage should meet 90% requirement per package.

- [ ] **Step 12: Commit**

```bash
git add src/
git commit -m "feat: add remaining unit tests for full JaCoCo coverage"
```
