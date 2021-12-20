-- Plan Destination table.
CREATE TABLE plan_destination (
  -- id of plan destination, it should be primary key.
  id                                 BIGINT                      NOT NULL,
  -- id provided by the probe.
  external_id                        VARCHAR(45)                 NOT NULL,
  -- latest time of discovery.
  discovery_time                     TIMESTAMP                   NOT NULL,
  -- latest time plan orchestrator update info of the plan destination.
  update_time                        TIMESTAMP                   NULL,
  -- status of plan destination.
  status                             VARCHAR(45)                 NOT NULL,
  -- contains all specified constraints when creating reservation.
  target_id                          BIGINT                      NOT NULL,
  -- The plan destination project info object, stored as raw protobuf.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/plan/PlanExportDTO.proto.
  plan_destination                   BLOB                        NOT NULL,

  PRIMARY KEY (id)
);

-- Create index for external id
CREATE INDEX idx_plan_destination_external_id on plan_destination(external_id);