-- We want to store the environment type of the groups and whether they are empty or not, to be able
-- to filter based on those criteria. Even though these information are unique for each group, we
-- do not add them in the grouping table since they are not primary characteristics of the group.

-- Table for storing supplementary characteristics of groups, like environment type and whether it
-- is empty or not
-- Cloud type is relevant only for groups with environment type CLOUD
CREATE TABLE group_supplementary_info (
    group_id BIGINT(20),
    empty BOOLEAN NOT NULL,
    environment_type INT(11) NOT NULL,
    cloud_type INT(11),
    PRIMARY KEY(group_id),
    CONSTRAINT fk_group_supplementary_info_grouping FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE CASCADE
);

