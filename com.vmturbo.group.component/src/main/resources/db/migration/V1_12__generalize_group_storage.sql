-- This script will refactor how we store groups in the database.
-- Change is triggered by introduction of resource groups

-- Table for storing tags associated with groups
CREATE TABLE group_tags (
    group_id BIGINT(20),
    tag_key VARCHAR(255),
    tag_value VARCHAR(255),
    PRIMARY KEY(group_id, tag_key, tag_value),
    consTraint fk_group_tags_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);

-- Table for storing expected entity member types
CREATE TABLE group_expected_members_entities (
    group_id BIGINT(20),
    entity_type INT(11),
    direct_member BOOLEAN NOT NULL,
    PRIMARY KEY (group_id, entity_type),
    CONSTRAINT fk_group_expected_members_entities_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);

-- Table to store expected gorup member types
CREATE TABLE group_expected_members_groups (
    group_id BIGINT(20),
    group_type INT(11),
    direct_member BOOLEAN NOT NULL,
    PRIMARY KEY (group_id, group_type),
    CONSTRAINT fk_group_expected_members_groups_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);

-- Table to store static members of a group (entities).
CREATE TABLE group_static_members_entities (
    group_id BIGINT(20),
    entity_type INT(11),
    entity_id BIGINT(20),
    PRIMARY KEY (group_id, entity_id),
    CONSTRAINT fk_group_static_members_entities_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);

-- Table to store static members of a group (groups).
CREATE TABLE group_static_members_groups (
    parent_group_id BIGINT(20),
    child_group_id BIGINT(20),
    PRIMARY KEY (parent_group_id, child_group_id),
    CONSTRAINT fk_group_static_members_groups_grouping FOREIGN KEY (parent_group_id) REFERENCES grouping (id) ON DELETE CASCADE,
    CONSTRAINT fk_group_static_members_groups_grouping_child FOREIGN KEY (child_group_id) REFERENCES grouping (id) ON DELETE CASCADE
);

-- Table to store targets that the group has been discovered by.
CREATE TABLE group_discover_targets (
    group_id BIGINT(20),
    target_id BIGINT(20),
    PRIMARY KEY (group_id, target_id),
    CONSTRAINT fk_group_discover_targets_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);

-- If true, this group can be looked up by the members
ALTER TABLE grouping ADD COLUMN supports_member_reverse_lookup boolean;
UPDATE grouping SET supports_member_reverse_lookup = false;
ALTER TABLE grouping MODIFY COLUMN supports_member_reverse_lookup boolean NOT NULL;

-- The username of the user who created this group (if created by user)
ALTER TABLE grouping ADD COLUMN origin_user_creator VARCHAR(255);
-- The UUID of this group across all targets that discovered it. (if discovered by target)
-- This is sometimes required when referencing this group in actions that go to the probes.
-- This is also used to join together groups across targets, and to distinguish between
-- re-discovered groups and newly discovered groups.
ALTER TABLE grouping ADD COLUMN origin_discovered_src_id VARCHAR(255);
-- A description of the purpose of this system group - mainly for clarity/debugging/logging
-- purposes. (if system-created groups)
ALTER TABLE grouping ADD COLUMN origin_system_description VARCHAR(255);
UPDATE grouping SET origin_user_creator = "unknown-user" WHERE origin = 1 OR origin IS NULL;
INSERT INTO group_discover_targets (group_id, target_id) (
        SELECT id, discovered_by_id
        FROM grouping
        WHERE origin = 2
        );
ALTER TABLE grouping DROP COLUMN discovered_by_id;
UPDATE grouping SET origin_discovered_src_id = name WHERE origin = 2;
ALTER TABLE grouping DROP COLUMN origin;

-- The type of group such as CLUSTER, RESOURCE GROUP, ...
ALTER TABLE grouping ADD COLUMN group_type INT(11);
-- Existing user groups are all regular groups. Existing discovered groups will be over
-- written by the next discovery cycle.
UPDATE grouping SET group_type = 0;
ALTER TABLE grouping MODIFY COLUMN group_type INT(11) NOT NULL;

-- The user-friendly name of the group.
ALTER TABLE grouping ADD COLUMN display_name VARCHAR(255);
UPDATE grouping SET display_name = name;
ALTER TABLE grouping DROP COLUMN name;
ALTER TABLE grouping MODIFY COLUMN display_name VARCHAR(255) NOT NULL;

-- If true, this group will not be displayed to users.
ALTER TABLE grouping ADD COLUMN is_hidden BOOLEAN;
UPDATE grouping SET is_hidden = FALSE;
ALTER TABLE grouping MODIFY COLUMN is_hidden BOOLEAN NOT NULL;

-- If there is an entity in the topology that "owns" this group, this is the ID of that
-- entity. The main use case (Sept 5 2019) is Business Accounts that own Resource Groups.
ALTER TABLE grouping ADD COLUMN owner_id BIGINT(20);
-- filters to retrieve this groups's children groups
ALTER TABLE grouping ADD COLUMN group_filters BLOB;
-- filters to retrieve this groups's children entities
ALTER TABLE grouping ADD COLUMN entity_filters BLOB;
ALTER TABLE grouping ADD COLUMN optimization_is_global_scope BOOLEAN;
ALTER TABLE grouping ADD COLUMN optimization_environment_type INT(11);
CREATE INDEX idx_grouping_display_name ON grouping (display_name);
CREATE INDEX idx_grouping_disc_src_id ON grouping (origin_discovered_src_id);

-- Obsolete migration V_01_00_01__Drop_Discovered_Groups_Policies
DELETE FROM policy WHERE discovered_by_id IS NOT NULL;
