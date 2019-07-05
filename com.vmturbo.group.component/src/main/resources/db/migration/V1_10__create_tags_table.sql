-- Information on tags
CREATE TABLE tags_group (
  -- The ID of the group being referenced.
  group_id             BIGINT         NOT NULL,

  -- Tag key and tag value
  tag_key              VARCHAR(255)   NOT NULL,
  tag_value            VARCHAR(255)   NOT NULL,

  PRIMARY KEY(group_id, tag_key, tag_value),

  CONSTRAINT fk_tags_group_id
    FOREIGN KEY(group_id) REFERENCES grouping(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
) ENGINE=INNODB DEFAULT CHARSET=utf8;