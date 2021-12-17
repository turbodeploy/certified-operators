-- Add the tag_origin column which shows the origin of the tag
ALTER TABLE group_tags ADD COLUMN tag_origin SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE group_tags DROP PRIMARY KEY, ADD PRIMARY KEY (group_id, tag_key, tag_value, tag_origin);
