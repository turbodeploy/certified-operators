-- A plan project describes the recurring plan created
CREATE TABLE plan_project (
  id                   BIGINT        NOT NULL,
  create_time          TIMESTAMP     NOT NULL,
  update_time          TIMESTAMP     NOT NULL,

  -- The plan project info object, stored as raw protobuf.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanDTO.proto
  project_info        BLOB          NOT NULL,

  PRIMARY KEY (id)
);