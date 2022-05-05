-- This script adds recommendation identity table which is used to persist OIDs for
-- market recommendation. In order to distinguish different market recommendations within
-- action instances

CREATE TABLE recommendation_identity (
    id BIGINT(20) PRIMARY KEY,
    action_type INT(2) NOT NULL,
    target_id BIGINT(20) NOT NULL,
    action_details VARCHAR(200) NULL,
    UNIQUE KEY (action_type, target_id, action_details)
);

ALTER TABLE action_history ADD COLUMN recommendation_oid BIGINT(20);
