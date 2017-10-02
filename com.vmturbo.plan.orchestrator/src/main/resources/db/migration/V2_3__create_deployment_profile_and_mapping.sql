CREATE TABLE deployment_profile (
  -- Id of deployment profile.
  id                          BIGINT         NOT NULL,
  -- Target Id of deployment profile, for user created deployment profile, it should be null.
  target_id                   BIGINT         DEFAULT NULL,
  -- Probe send unique Id for deployment profile, for user created deployment profile, it should be null.
  probe_deployment_profile_id VARCHAR(255)   DEFAULT NULL,
  -- Name of deployment profile.
  name                        VARCHAR(255)   NOT NULL,
  -- All the deployment profile fields stored as raw protobuf written to disk.
  deployment_profile_info     BLOB           NOT NULL,
  -- Id as primary key.
  PRIMARY KEY (id)
);

-- This table maintain many to many relationship between template with deployment profile tables
CREATE TABLE template_to_deployment_profile (
  -- Id of template
  template_id                BIGINT      NOT NULL,
  -- Id of deployment profile
  deployment_profile_id      BIGINT      NOT NULL,
  -- Create template id as a foreign key of Template table
  FOREIGN  KEY (template_id) REFERENCES template(id) ON DELETE CASCADE,
  -- Create deployment profile id as a foreign key of Deployment profile table.
  FOREIGN  KEY (deployment_profile_id) REFERENCES deployment_profile(id) ON DELETE CASCADE,
  -- Combination of template id and deployment profile id as primary key.
  PRIMARY KEY (template_id, deployment_profile_id)
)