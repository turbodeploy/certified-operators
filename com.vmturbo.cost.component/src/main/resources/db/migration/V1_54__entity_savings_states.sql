-- Create entity_savings_state table for storing states used for savings calculations.
CREATE TABLE entity_savings_state (
    entity_oid BIGINT(20) NOT NULL,
    entity_state MEDIUMTEXT DEFAULT NULL CHECK (json_valid(`entity_state`)),
    PRIMARY KEY (entity_oid)
);