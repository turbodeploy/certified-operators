-- A setting_policy binds a group of settings to one or more groups of entities that those settings
-- should apply to.
--
-- For more information on setting policies see:
-- https://vmturbo.atlassian.net/wiki/spaces/Home/pages/172656109/Settings+in+XL
CREATE TABLE setting_policies (
  -- The ID assigned to a setting policy.
  id                   BIGINT        NOT NULL,

  -- The name of the policy. The name is required to be unique.
  name                 VARCHAR(255)  NOT NULL UNIQUE,

  -- The entity type that this setting policy applies to.
  -- Multiple setting policies may apply to the same entity type.
  entity_type          INT,

  -- A binary blob containing the raw protobuf which provides additional details
  -- about the setting policy.
  setting_policy_data  BLOB          NOT NULL,

  PRIMARY KEY (id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;
