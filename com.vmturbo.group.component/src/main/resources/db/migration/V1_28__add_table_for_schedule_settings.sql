-- Create new table in order to support multiple execution windows per (actionType - actionMode) pair.
CREATE TABLE setting_policy_setting_schedule_ids (
    policy_id BIGINT(20) NOT NULL,
    setting_name VARCHAR(255) NOT NULL,
    execution_schedule_id bigint(20) NOT NULL,
    PRIMARY KEY (policy_id, setting_name, execution_schedule_id),
    CONSTRAINT fk_setting_policy_setting_schedule_ids FOREIGN KEY (policy_id, setting_name)
        REFERENCES setting_policy_setting(policy_id, setting_name) ON DELETE CASCADE,
    CONSTRAINT fk_execution_schedule_id FOREIGN KEY (execution_schedule_id) REFERENCES schedule (id) ON DELETE RESTRICT
);
