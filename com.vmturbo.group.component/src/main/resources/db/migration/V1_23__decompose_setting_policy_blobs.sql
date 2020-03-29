-- This SQL is dedicated to expose all the data from the internal blob of setting_policy table

UPDATE setting_policy SET target_id=NULL WHERE policy_type <> 'discovered';

ALTER TABLE setting_policy ADD COLUMN display_name VARCHAR(255);
ALTER TABLE setting_policy ADD COLUMN enabled BOOLEAN;
ALTER TABLE setting_policy ADD COLUMN schedule_id BIGINT(20);
ALTER TABLE setting_policy ADD CONSTRAINT setting_policy_schedule FOREIGN KEY (schedule_id)
        REFERENCES schedule(id);

CREATE TABLE setting_policy_groups (
    group_id BIGINT(20) NOT NULL,
    setting_policy_id BIGINT(20) NOT NULL,
    FOREIGN KEY setting_policy_groups_groups (group_id) REFERENCES grouping(id) ON DELETE CASCADE,
    FOREIGN KEY setting_policy_groups_policies (setting_policy_id)
        REFERENCES setting_policy(id) ON DELETE CASCADE
);

CREATE TABLE setting_policy_setting (
    policy_id BIGINT(20) NOT NULL,
    setting_name VARCHAR(255) NOT NULL,
    setting_type SMALLINT NOT NULL,
    setting_value VARCHAR(255) NOT NULL,
    PRIMARY KEY (policy_id, setting_name),
    CONSTRAINT fk_setting_policy_setting_group FOREIGN KEY (policy_id)
        REFERENCES setting_policy(id) ON DELETE CASCADE
);

CREATE TABLE setting_policy_setting_oids (
    policy_id BIGINT(20) NOT NULL,
    setting_name VARCHAR(255) NOT NULL,
    oid_number INTEGER NOT NULL,
    oid BIGINT(20) NOT NULL,
    PRIMARY KEY (policy_id, setting_name, oid_number),
    CONSTRAINT fk_setting_policy_setting_oids_settings FOREIGN KEY (policy_id, setting_name)
        REFERENCES setting_policy_setting(policy_id, setting_name) ON DELETE CASCADE
);

UPDATE setting_policy t
    SET schedule_id = (
        SELECT MAX (schedule_id)
        FROM setting_policy_schedule s
        WHERE t.id = s.setting_policy_id
        );
DROP TABLE setting_policy_schedule;

