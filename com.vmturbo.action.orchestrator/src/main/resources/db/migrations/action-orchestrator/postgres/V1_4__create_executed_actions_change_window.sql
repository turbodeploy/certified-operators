-- Keeps track of timeframe information related to successfully executed actions.
-- The change window is the timeframe in which the action is having an impact on the entity.
-- Events such as another action execution for the same entity, or an external revert of the action,
-- would end the "change window".
CREATE TABLE executed_actions_change_window (
  -- the action id.
  action_oid BIGINT NOT NULL,
  -- the target entity OID.
  entity_oid BIGINT NOT NULL,
  -- the time of successful execution.
  start_time TIMESTAMP NOT NULL,
  -- the end time of the change window
  end_time TIMESTAMP NULL,
  -- the reason why the action stopped being relevant at end_time (SUPERSEDED, REVERTED, ..).
  termination_reason INTEGER,
  -- the primary key.  start_time would be non-unique in the case of reverted actions.
  PRIMARY KEY (action_oid, start_time)
);