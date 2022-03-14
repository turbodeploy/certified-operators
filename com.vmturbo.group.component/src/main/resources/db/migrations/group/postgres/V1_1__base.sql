CREATE table IF NOT EXISTS auto_topo_data_defs (
  id bigint NOT NULL,
  name_prefix varchar(255) NOT NULL,
  entity_type integer NOT NULL,
  connected_entity_type integer NOT NULL,
  tag_key varchar(255) NOT NULL,
  PRIMARY KEY (id)
);


-- group_component.entity_custom_tags definition

CREATE TABLE IF NOT EXISTS entity_custom_tags (
  entity_id bigint NOT NULL,
  tag_key varchar(255) NOT NULL,
  tag_value varchar(255) NOT NULL,
  PRIMARY KEY (entity_id,tag_key,tag_value)
) ;


-- group_component.global_settings definition

CREATE TABLE IF NOT EXISTS global_settings (
  name varchar(255) NOT NULL,
  setting_data bytea NOT NULL,
  PRIMARY KEY (name)
) ;


CREATE TABLE IF NOT EXISTS grouping (
  id bigint NOT NULL,
  supports_member_reverse_lookup boolean NOT NULL,
  origin_user_creator varchar(255) DEFAULT NULL,
  origin_discovered_src_id varchar(255) DEFAULT NULL,
  origin_system_description varchar(255) DEFAULT NULL,
  group_type integer NOT NULL,
  display_name varchar(255) NOT NULL,
  is_hidden boolean NOT NULL,
  owner_id bigint DEFAULT NULL,
  group_filters bytea DEFAULT NULL,
  entity_filters bytea DEFAULT NULL,
  optimization_is_global_scope boolean DEFAULT NULL,
  optimization_environment_type integer DEFAULT NULL,
  hash bytea DEFAULT NULL,
  stitch_across_targets boolean DEFAULT FALSE,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_grouping_display_name ON grouping (display_name);
CREATE INDEX IF NOT EXISTS idx_grouping_disc_src_id ON grouping (origin_discovered_src_id);

-- group_component.manual_topo_data_defs definition

CREATE TABLE IF NOT EXISTS manual_topo_data_defs (
  id bigint NOT NULL,
  name varchar(255) NOT NULL,
  entity_type integer NOT NULL,
  context_based boolean DEFAULT FALSE,
  PRIMARY KEY (id)
) ;


-- group_component.policy definition

CREATE TABLE IF NOT EXISTS policy (
  id bigint NOT NULL UNIQUE,
  name varchar(255) NOT null UNIQUE,
  enabled boolean NOT NULL,
  discovered_by_id bigint DEFAULT NULL,
  policy_data bytea NOT NULL,
  hash bytea DEFAULT NULL,
  display_name varchar(255) NOT NULL,
  PRIMARY KEY (id)
) ;


-- group_component.schedule definition

CREATE TABLE IF NOT EXISTS schedule (
  id bigint NOT NULL,
  display_name varchar(255) NOT NULL UNIQUE,
  start_time timestamp NOT NULL DEFAULT current_timestamp(3),
  end_time timestamp NOT NULL DEFAULT NULLIF('0000-00-00 00:00:00','0000-00-00 00:00:00')::timestamp,
  last_date timestamp NULL DEFAULT NULL,
  recur_rule varchar(80) DEFAULT NULL,
  time_zone_id varchar(40) NOT NULL,
  recurrence_start_time timestamp NULL DEFAULT NULL,
  delete_after_expiration boolean DEFAULT FALSE,
  PRIMARY KEY (id)
) ;


-- group_component.settings definition

CREATE TABLE IF NOT EXISTS settings (
  id SERIAL NOT NULL,
  topology_context_id bigint NOT NULL,
  setting_name varchar(255) NOT NULL,
  setting_type smallint NOT NULL,
  setting_value varchar(255) NOT NULL,
  PRIMARY KEY (id)
 );


-- group_component.topology_data_definition_oid definition

CREATE TABLE IF NOT EXISTS topology_data_definition_oid (
  id bigint NOT NULL,
  identity_matching_attributes text NOT NULL,
  PRIMARY KEY (id)
) ;


-- group_component.entity_settings definition

CREATE TABLE IF NOT EXISTS entity_settings (
  topology_context_id bigint NOT NULL,
  entity_id bigint NOT NULL,
  setting_id integer NOT NULL,
  PRIMARY KEY (topology_context_id,entity_id,setting_id),
  CONSTRAINT fk_setting FOREIGN KEY (setting_id) REFERENCES settings (id) ON DELETE CASCADE
);


-- group_component.group_discover_targets definition

CREATE TABLE IF NOT EXISTS group_discover_targets (
  group_id bigint NOT NULL,
  target_id bigint NOT NULL,
  PRIMARY KEY (group_id,target_id),
  CONSTRAINT fk_group_discover_targets_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);


-- group_component.group_expected_members_entities definition

CREATE TABLE IF NOT EXISTS group_expected_members_entities (
  group_id bigint NOT NULL,
  entity_type integer NOT NULL,
  direct_member boolean NOT NULL,
  PRIMARY KEY (group_id,entity_type),
  CONSTRAINT fk_group_expected_members_entities_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);


-- group_component.group_expected_members_groups definition

CREATE TABLE IF NOT EXISTS group_expected_members_groups (
  group_id bigint NOT NULL,
  group_type integer NOT NULL,
  direct_member boolean NOT NULL,
  PRIMARY KEY (group_id,group_type),
  CONSTRAINT fk_group_expected_members_groups_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);


-- group_component.group_static_members_entities definition

CREATE TABLE IF NOT EXISTS group_static_members_entities (
  group_id bigint NOT NULL,
  entity_type integer DEFAULT NULL,
  entity_id bigint NOT NULL,
  PRIMARY KEY (group_id,entity_id),
  CONSTRAINT fk_group_static_members_entities_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);


-- group_component.group_static_members_groups definition

CREATE TABLE IF NOT EXISTS group_static_members_groups (
  parent_group_id bigint NOT NULL,
  child_group_id bigint NOT NULL,
  PRIMARY KEY (parent_group_id,child_group_id),
  CONSTRAINT fk_group_static_members_groups_grouping FOREIGN KEY (parent_group_id) REFERENCES grouping (id) ON DELETE CASCADE,
  CONSTRAINT fk_group_static_members_groups_grouping_child FOREIGN KEY (child_group_id) REFERENCES grouping (id) ON DELETE CASCADE
);


-- group_component.group_supplementary_info definition

CREATE TABLE IF NOT EXISTS group_supplementary_info (
  group_id bigint NOT NULL,
  empty boolean NOT NULL,
  environment_type integer NOT NULL,
  cloud_type integer DEFAULT NULL,
  severity integer DEFAULT NULL,
  PRIMARY KEY (group_id),
  CONSTRAINT fk_group_supplementary_info_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);


-- group_component.group_tags definition

CREATE TABLE IF NOT EXISTS group_tags (
  group_id bigint NOT NULL,
  tag_key varchar(255) NOT NULL,
  tag_value varchar(255) NOT NULL,
  tag_origin smallint NOT NULL DEFAULT 1,
  PRIMARY KEY (group_id,tag_key,tag_value,tag_origin),
  CONSTRAINT fk_group_tags_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);


-- group_component.man_data_defs_assoc_entities definition

CREATE TABLE IF NOT EXISTS man_data_defs_assoc_entities (
  definition_id bigint NOT NULL,
  entity_type integer DEFAULT NULL,
  entity_id bigint NOT NULL,
  PRIMARY KEY (definition_id,entity_id),
  CONSTRAINT fk_man_data_defs_assoc_entities_manual_topo_data_defs FOREIGN KEY (definition_id) REFERENCES manual_topo_data_defs (id) ON DELETE CASCADE
);


-- group_component.man_data_defs_assoc_groups definition

CREATE TABLE IF NOT EXISTS man_data_defs_assoc_groups (
  definition_id bigint NOT NULL,
  group_oid bigint NOT NULL,
  entity_type integer DEFAULT NULL,
  PRIMARY KEY (definition_id,group_oid),
  CONSTRAINT fk_man_data_defs_assoc_groups_manual_topo_data_defs FOREIGN KEY (definition_id) REFERENCES manual_topo_data_defs (id) ON DELETE CASCADE
);


-- group_component.man_data_defs_dyn_connect_filters definition

CREATE TABLE IF NOT EXISTS man_data_defs_dyn_connect_filters (
  definition_id bigint NOT NULL,
  entity_type integer NOT NULL,
  dynamic_connection_filters bytea NOT NULL,
  PRIMARY KEY (definition_id,entity_type),
  CONSTRAINT fk_man_data_defs_dyn_connect_filters_manual_topo_data_defs FOREIGN KEY (definition_id) REFERENCES manual_topo_data_defs (id) ON DELETE CASCADE
);


-- group_component.policy_group definition

CREATE table IF NOT EXISTS policy_group (
  policy_id bigint NOT NULL,
  group_id bigint NOT NULL,
  PRIMARY KEY (group_id,policy_id),
  CONSTRAINT fk_group_id FOREIGN KEY (group_id) REFERENCES grouping (id),
  CONSTRAINT fk_policy_id FOREIGN KEY (policy_id) REFERENCES policy (id) ON DELETE CASCADE ON UPDATE CASCADE
);


-- Do it this way to make the statement idempotent
DO $$ BEGIN
    CREATE TYPE policy_type AS ENUM (
    'user',
    'default',
    'discovered'
);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- group_component.setting_policy definition

CREATE TABLE IF NOT EXISTS setting_policy (
  id bigint NOT NULL,
  name varchar(255) NOT NULL,
  entity_type integer NOT NULL,
  policy_type policy_type NOT NULL,
  target_id bigint DEFAULT NULL,
  display_name varchar(255) DEFAULT NULL,
  enabled boolean NOT NULL,
  schedule_id bigint DEFAULT NULL,
  hash bytea DEFAULT NULL,
  delete_after_expiration boolean DEFAULT FALSE,
  PRIMARY KEY (id),
  CONSTRAINT setting_policy_schedule FOREIGN KEY (schedule_id) REFERENCES schedule (id)
) ;


-- group_component.setting_policy_groups definition

CREATE TABLE IF NOT EXISTS setting_policy_groups (
  group_id bigint NOT NULL,
  setting_policy_id bigint NOT NULL,
  CONSTRAINT setting_policy_groups_groups FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE,
  CONSTRAINT setting_policy_groups_policies FOREIGN KEY (setting_policy_id) REFERENCES setting_policy (id) ON DELETE CASCADE
);


-- group_component.setting_policy_setting definition

CREATE TABLE IF NOT EXISTS setting_policy_setting (
  policy_id bigint NOT NULL,
  setting_name varchar(255) NOT NULL,
  setting_type smallint NOT NULL,
  setting_value varchar(255) NOT NULL,
  PRIMARY KEY (policy_id,setting_name),
  CONSTRAINT fk_setting_policy_setting_group FOREIGN KEY (policy_id) REFERENCES setting_policy (id) ON DELETE CASCADE
);


-- group_component.setting_policy_setting_oids definition

CREATE TABLE IF NOT EXISTS setting_policy_setting_oids (
  policy_id bigint NOT NULL,
  setting_name varchar(255) NULL,
  oid_number integer NOT NULL,
  oid bigint NOT NULL,
  PRIMARY KEY (policy_id,setting_name,oid_number),
  CONSTRAINT fk_setting_policy_setting_oids_settings FOREIGN KEY (policy_id, setting_name) REFERENCES setting_policy_setting (policy_id, setting_name) ON DELETE CASCADE
) ;


-- group_component.setting_policy_setting_schedule_ids definition

CREATE TABLE IF NOT EXISTS setting_policy_setting_schedule_ids (
  policy_id bigint NOT NULL,
  setting_name varchar(255) NOT NULL,
  execution_schedule_id bigint NOT NULL,
  PRIMARY KEY (policy_id,setting_name,execution_schedule_id),
  CONSTRAINT fk_execution_schedule_id FOREIGN KEY (execution_schedule_id) REFERENCES schedule (id),
  CONSTRAINT fk_setting_policy_setting_schedule_ids FOREIGN KEY (policy_id, setting_name) REFERENCES setting_policy_setting (policy_id, setting_name) ON DELETE CASCADE
) ;


-- group_component.settings_oids definition

CREATE TABLE IF NOT EXISTS settings_oids (
  setting_id SERIAL NOT NULL,
  oid bigint NOT NULL,
  PRIMARY KEY (setting_id,oid),
  CONSTRAINT fk_setting_id FOREIGN KEY (setting_id) REFERENCES settings (id) ON DELETE CASCADE
);


-- group_component.settings_policies definition

CREATE TABLE IF NOT EXISTS settings_policies (
  setting_id SERIAL NOT NULL,
  policy_id bigint NOT NULL,
  PRIMARY KEY (setting_id,policy_id),
  CONSTRAINT fk_settings_policies_setting_id FOREIGN KEY (setting_id) REFERENCES settings (id) ON DELETE CASCADE
);


-- group_component.tags_group definition

CREATE TABLE IF NOT EXISTS tags_group (
  group_id bigint NOT NULL,
  tag_key varchar(255) NOT NULL,
  tag_value varchar(255) NOT NULL,
  PRIMARY KEY (group_id,tag_key,tag_value),
  CONSTRAINT fk_tags_group_id FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE ON UPDATE CASCADE
) ;

DROP VIEW IF EXISTS all_topo_data_defs;
CREATE VIEW all_topo_data_defs  AS
  SELECT id, name AS name_or_prefix, 'MANUAL' AS type
  FROM manual_topo_data_defs
UNION
  SELECT id, name_prefix AS name_or_prefix, 'AUTO' AS type
  FROM auto_topo_data_defs;
