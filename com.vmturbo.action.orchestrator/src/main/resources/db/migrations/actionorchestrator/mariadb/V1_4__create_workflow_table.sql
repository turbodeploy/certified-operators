-- the workflow table depends on workflow_oids, and so must be deleted first
DROP TABLE IF EXISTS workflow;
DROP TABLE IF EXISTS workflow_oid;

-- persist OIDs for discovered Workflow objects here. The primary key is 'id', 'target_id', and 'external_name'.
CREATE TABLE workflow_oid (

  -- unique ID for this Workflow
  id BIGINT NOT NULL,

  -- OID for the target from which this Workflow was discovered
  target_id BIGINT NOT NULL,

  -- The "external name" captured from the target - the name given by the user via the target UI
  external_name VARCHAR(255) COLLATE utf8_unicode_ci NOT NULL,

  -- the pair (target_id, external_name) must be unique
  UNIQUE workflow_target_name_unique (target_id, external_name),

  -- the unique ID for this Workflow
  PRIMARY KEY (id),
  INDEX workflow_target_id_idx (target_id),
  INDEX workflow_target_id_and_external_name_idx (target_id,external_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- a table to hold the 'workflowinfo' protobuf, indexed by the OID
CREATE TABLE workflow (
  -- the OID for the workflow
  id BIGINT NOT NULL,

  -- The binary form of the WorkflowInfo to be used (later) to extract the workflow parameters
  workflow_info BLOB,

  -- When this Workflow was first discovered
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  -- The most recent time this workflow was discovered
  last_update_time TIMESTAMP NOT NULL DEFAULT 0,

  -- the ID lives in the workflow_oids table; auto delete the workflow_param if the OID is deleted
  FOREIGN KEY (id) REFERENCES workflow_oid(id) ON DELETE CASCADE

) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;