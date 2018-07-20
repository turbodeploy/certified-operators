-- This tables stores entity controllable flag information. When there is action sent to probe,
-- it will create records with 'queued' status, and when the action is being executed by porbe, it
-- will update its status to 'in progress' and when the action is executed successfully, it will update
-- its status to 'succeed', and when the action is failed, it will deleted all related records.
CREATE TABLE entity_action (
  -- Id of a action, it should be a Move action id.
  action_id                   BIGINT                                    NOT NULL,

  -- Id of entity id, it should be a provider entity of above action Id.
  entity_id                   BIGINT                                    NOT NULL,

  -- There are three status: 'queued', 'in progress', 'succeed'. 1: 'queued' means this action has
  -- been sent to probe, but not start executed yet. 2: 'in progress' means this action is being
  -- executed now by probe. 3: 'succeed' means this action is executed successfully. 4: 'failed' means
  -- this action execution is failed by probe.
  status                      ENUM('queued', 'in progress', 'succeed', 'failed')  NOT NULL,

  -- The time when last update happens.
  update_time                 TIMESTAMP                                 NOT NULL,

  -- Use action id and entity id as combination key.
  PRIMARY KEY (action_id, entity_id)
);