-- Table to store created reporting schedules.
create table schedule (
    id BIGINT NOT NULL,

    -- Day of week when report should be generated
    day_of_week NVARCHAR(10) NOT NULL,

    -- PDF or XLSX
    format NVARCHAR(10) NOT NULL,

    -- Daily, weekly or monthly
    period NVARCHAR(10) NOT NULL,

    report_type SMALLINT NOT NULL,

    -- Uuid of the scope (group, cluster, etc) if scope is presented
    scope_oid NVARCHAR(50) NULL,

    show_charts BOOLEAN NOT NULL,

    -- Report template id (reference to vmtdb DB)
    template_id INTEGER NOT NULL,
    PRIMARY KEY (id)
);

-- Table to store email adresses of schedule subscribers. Each schedule may have several subscribers
-- or may not have any subscribers.
create table schedule_subscribers (
    -- Reference to 'schedule' table.
    schedule_id BIGINT NOT NULL,

    -- Subscriber's email.
    email_address NVARCHAR(100) NOT NULL,
    FOREIGN KEY (schedule_id) REFERENCES schedule(id) ON DELETE CASCADE
);