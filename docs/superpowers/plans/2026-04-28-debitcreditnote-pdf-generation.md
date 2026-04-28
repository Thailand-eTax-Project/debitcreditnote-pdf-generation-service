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
