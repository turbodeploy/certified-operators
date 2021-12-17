-- Information on user defined entity tags
CREATE TABLE entity_custom_tags (
  -- The ID of the entity being referenced.
  entity_id            BIGINT         NOT NULL,

  -- Tag key and tag value
  tag_key              VARCHAR(255)   NOT NULL,
  tag_value            VARCHAR(255)   NOT NULL,

  PRIMARY KEY(entity_id, tag_key, tag_value)
) ENGINE=INNODB DEFAULT CHARSET=utf8;
