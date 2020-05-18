-- Tables for storing settings used by plans (OM-55733)
CREATE TABLE settings (
    id int(11) NOT NULL AUTO_INCREMENT,
    topology_context_id bigint(20) NOT NULL,
    setting_name varchar(255) NOT NULL,
    setting_type smallint(6) NOT NULL,
    setting_value varchar(255) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE settings_oids (
    setting_id int(11) NOT NULL AUTO_INCREMENT,
    oid bigint(20) NOT NULL,
    PRIMARY KEY (setting_id, oid),
    CONSTRAINT fk_setting_id FOREIGN KEY (setting_id) REFERENCES settings (id) ON DELETE CASCADE
);

CREATE TABLE entity_settings (
    topology_context_id bigint(20) NOT NULL,
    entity_id bigint(20) NOT NULL,
    setting_id int(11) NOT NULL,
    PRIMARY KEY (topology_context_id, entity_id, setting_id),
    CONSTRAINT fk_setting FOREIGN KEY (setting_id) REFERENCES settings (id) ON DELETE CASCADE
);
