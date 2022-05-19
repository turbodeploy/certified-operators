-- We don't care about old records, so drop the table.
DROP TABLE IF EXISTS executed_actions_change_window;

-- Action liveness tracking table.
CREATE TABLE executed_actions_change_window (
  -- Action id, unique for each executed action.
  action_oid BIGINT NOT NULL,
  -- Target entity OID.
  entity_oid BIGINT NOT NULL,
  -- Topology broadcast time when we detect, via topology, that action has been executed.
  start_time TIMESTAMP NULL,
  -- End time of the change window. Set when we detect action as no longer live.
  end_time TIMESTAMP NULL,
  -- States to keep track of action liveness (SUPERSEDED, REVERTED, ..).
  -- Default value is LivenessState.NEW = 1.
  liveness_state INTEGER NOT NULL DEFAULT 1,
  -- Action oid, there is expected to be only 1 record with a given action oid at a time.
  PRIMARY KEY (action_oid),
  INDEX executed_actions_change_window_idx_eoid (entity_oid),
  INDEX executed_actions_change_window_idx_lsaid (liveness_state, action_oid)
);
