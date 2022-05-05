-- This tables stores the validation and discovery stages for individual targets.

DROP TABLE IF EXISTS target_status;
CREATE TABLE target_status (
  -- The target unique id.
  target_id      BIGINT            NOT NULL,

  -- Blob containing the TargetStatus protobuf containing the validation/discovery stages for the
  -- target, along with any additional information.
  status         MEDIUMBLOB        NULL,

  PRIMARY KEY (target_id)
);
