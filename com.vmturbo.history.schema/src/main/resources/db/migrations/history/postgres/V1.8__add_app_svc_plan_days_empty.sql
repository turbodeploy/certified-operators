CREATE TABLE IF NOT EXISTS application_service_days_empty (
    id bigint NOT NULL,
    name varchar(80) COLLATE ci DEFAULT NULL,
    first_discovered timestamp NOT NULL,
    last_discovered timestamp NOT NULL,
    PRIMARY KEY (id)
);