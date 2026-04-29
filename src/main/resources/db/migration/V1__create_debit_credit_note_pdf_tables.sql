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
