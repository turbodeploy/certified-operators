-- A plan instance describes the plan analysis created
CREATE TABLE plan_instance (
  id                   BIGINT        NOT NULL,
  create_time          TIMESTAMP     NOT NULL,
  update_time          TIMESTAMP     NOT NULL,

  -- The plan instance object, stored as raw protobuf.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanDTO.proto
  plan_instance        BLOB          NOT NULL,

  PRIMARY KEY (id)
);