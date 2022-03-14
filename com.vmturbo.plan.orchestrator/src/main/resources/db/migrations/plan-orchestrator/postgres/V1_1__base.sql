-- A template describes the pattern of service entity which created by user or discovered by Probe.
CREATE TABLE IF NOT EXISTS template (
  -- The id of template
  id                  BIGINT         NOT NULL,
  -- The target id of template, if it's user created template, it will be null.
  target_id           BIGINT         DEFAULT NULL,
  -- The name of template.
  name                VARCHAR(255)   NOT NULL,
  -- The type of service entity.
  entity_type         integer        NOT NULL,
  -- All the template fields stored as raw protobuf written to disk.
  template_info       bytea          NOT NULL,
  probe_template_id   VARCHAR(255) DEFAULT NULL,
  type VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS deployment_profile (
  -- Id of deployment profile.
  id                          BIGINT         NOT NULL,
  -- Target Id of deployment profile, for user created deployment profile, it should be null.
  target_id                   BIGINT         DEFAULT NULL,
  -- Probe send unique Id for deployment profile, for user created deployment profile, it should be null.
  probe_deployment_profile_id VARCHAR(255)   DEFAULT NULL,
  -- Name of deployment profile.
  name                        VARCHAR(255)   NOT NULL,
  -- All the deployment profile fields stored as raw protobuf written to disk.
  deployment_profile_info     bytea          NOT NULL,
  -- Id as primary key.
  PRIMARY KEY (id)
);

-- This table maintain many to many relationship between template with deployment profile tables
CREATE TABLE IF NOT EXISTS template_to_deployment_profile (
  -- Id of template
  template_id                BIGINT      NOT NULL,
  -- Id of deployment profile
  deployment_profile_id      BIGINT      NOT NULL,
  -- Create template id as a foreign key of Template table
  CONSTRAINT template_id_fk FOREIGN  KEY (template_id) REFERENCES template(id) ON DELETE CASCADE,
  -- Create deployment profile id as a foreign key of Deployment profile table.
  CONSTRAINT deployment_profile_id_fk FOREIGN  KEY (deployment_profile_id) REFERENCES deployment_profile(id) ON DELETE CASCADE,
  -- Combination of template id and deployment profile id as primary key.
  PRIMARY KEY (template_id, deployment_profile_id)
);

-- This table keeps the id for cluster headroom template for a group.
CREATE TABLE IF NOT EXISTS cluster_to_headroom_template_id (
    -- Id of the group
    group_id                   BIGINT         NOT NULL,
    -- Id of the template
    template_id                BIGINT         NOT NULL,
    -- Create template id as a foreign key of Template table.
    -- FOREIGN  KEY (template_id) REFERENCES template(id) ON DELETE CASCADE,
    CONSTRAINT template_id_fk FOREIGN KEY(template_id) REFERENCES template(id) ON DELETE CASCADE,
    -- Group id and template id as primary key since one cluster has one head room template.
    PRIMARY KEY (group_id)
);

-- Plan Destination table.
CREATE TABLE IF NOT EXISTS plan_destination (
  -- id of plan destination, it should be primary key.
  id                                 BIGINT                      NOT NULL,
  -- id provided by the probe.
  external_id                        VARCHAR(80)                 NOT NULL,
  -- latest time of discovery.
  discovery_time                     TIMESTAMP                   NOT NULL DEFAULT now(),
  -- latest time plan orchestrator update info of the plan destination.
  update_time                        TIMESTAMP                   NULL,
  -- status of plan destination.
  status                             VARCHAR(45)                 NOT NULL,
  -- contains all specified constraints when creating reservation.
  target_id                          BIGINT                      NOT NULL,
  -- The plan destination project info object, stored as raw protobuf.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanExportDTO.proto.
  plan_destination                   bytea                       NOT NULL,

  PRIMARY KEY (id)
);

-- Create index for external id
CREATE INDEX IF NOT EXISTS idx_plan_destination_external_id on plan_destination(external_id);

-- A plan instance describes the plan analysis created
CREATE TABLE IF NOT EXISTS plan_instance (
  id                   BIGINT         NOT NULL,
  create_time          TIMESTAMP      NOT NULL DEFAULT now(),
  update_time          TIMESTAMP      NOT NULL,

  -- The plan instance object, stored as raw protobuf.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanDTO.proto
  plan_instance        bytea          NOT NULL,
  type                 VARCHAR(20)    DEFAULT NULL,
  status               VARCHAR(30)    DEFAULT NULL,
  PRIMARY KEY (id)
);

-- A plan project describes the recurring plan created
CREATE TABLE IF NOT EXISTS plan_project (
  id                   BIGINT         NOT NULL,
  create_time          TIMESTAMP      NOT NULL DEFAULT now(),
  update_time          TIMESTAMP      NOT NULL,

  -- The plan project info object, stored as raw protobuf.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanDTO.proto
  project_info        bytea           NOT NULL,
  type                 VARCHAR(20)    DEFAULT NULL,
  status               VARCHAR(30)    DEFAULT NULL,
  PRIMARY KEY (id)
);

-- Reservation table contains all user created reservations.
CREATE TABLE IF NOT EXISTS reservation (
  -- id of reservation, it should be primary key.
  id                                 BIGINT                       NOT NULL,
  -- name of reservation.
  name                               VARCHAR(255)                 NOT NULL,
  -- start time of reservation.
  start_time                         TIMESTAMP                    NOT NULL DEFAULT now(),
  -- expire time of reservation.
  expire_time                        TIMESTAMP                    NOT NULL,
  -- status of reservation.
  status                             integer                      NOT NULL,
  deployed                           integer                      NOT NULL,
  -- contains all reservation instances by different templates.
  reservation_template_collection    bytea                        NULL,
  -- contains all specified constraints when creating reservation.
  constraint_info_collection         bytea                        DEFAULT NULL,
  mode                               integer                      NOT NULL,
  grouping                           integer                      NOT NULL,
  PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS reservation_name_unique ON reservation(name);

-- A scenario describes the changes to be made to a given topology and set of configurations
-- before running a market analysis (plan)
CREATE TABLE IF NOT EXISTS scenario (
  id                   BIGINT        NOT NULL,
  create_time          TIMESTAMP     NOT NULL DEFAULT now(),
  update_time          TIMESTAMP     NOT NULL,

  -- The set of changes associated with the topology.
  -- Changes are stored as raw protobuf written to disk.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanDTO.proto
  scenario_info         bytea        NOT NULL,

  PRIMARY KEY (id)
);