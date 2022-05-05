-- A scenario describes the changes to be made to a given topology and set of configurations
-- before running a market analysis (plan)
CREATE TABLE scenario (
  id                   BIGINT        NOT NULL,
  create_time          TIMESTAMP     NOT NULL,
  update_time          TIMESTAMP     NOT NULL,

  -- The set of changes associated with the topology.
  -- Changes are stored as raw protobuf written to disk.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanDTO.proto
  scenario_spec         BLOB          NOT NULL,

  PRIMARY KEY (id)
);