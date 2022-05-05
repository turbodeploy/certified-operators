-- A template describes the pattern of service entity which created by user or discovered by Probe.
CREATE TABLE template (
  -- The id of template
  id                  BIGINT         NOT NULL,

  -- The target id of template, if it's user created template, it will be null.
  target_id           BIGINT         DEFAULT NULL,

  -- The name of template.
  name                VARCHAR(255)   NOT NULL,

  -- The type of service entity.
  template_type       INTEGER        NOT NULL,

  -- All the template fields stored as raw protobuf written to disk.
  template_info       BLOB           NOT NULL,

  PRIMARY KEY (id)
);