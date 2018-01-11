-- Table to store generated reports metadata (everything but bytes)
create table report_instance (
    -- id also identifies the file name, which is storing the report data
    id BIGINT NOT NULL,
    -- Timestamp of report generation
    generation_time datetime,
    -- Report template id (reference to vmtdb DB)
    template_id INTEGER NOT NULL,
    -- Flag, whether this record is dirty (0) or committed (1)
    committed BOOLEAN NOT NULL,
    PRIMARY KEY (id)
);
