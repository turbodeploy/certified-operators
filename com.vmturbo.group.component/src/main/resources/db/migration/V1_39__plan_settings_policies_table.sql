-- Tables for storing policies that contributes to resulting settings used by plans
CREATE TABLE settings_policies (
    setting_id int(11) NOT NULL AUTO_INCREMENT,
    policy_id bigint(20) NOT NULL,
    PRIMARY KEY (setting_id, policy_id),
    CONSTRAINT fk_settings_policies_setting_id FOREIGN KEY (setting_id) REFERENCES settings (id) ON DELETE CASCADE
);
