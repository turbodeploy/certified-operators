-- A action history consists of the executed com.vmturbo.action.orchestrator.action.Action.
-- A action history is immutable.
-- It does not contain ActionTranslation property in Action, since we don't need translation anymore.
-- Actions are grouped together into an action history.
CREATE TABLE action_history (
  -- The id of the action.
  id                   BIGINT        NOT NULL,

  create_time          TIMESTAMP     NOT NULL,

  update_time          TIMESTAMP     NOT NULL,

  -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
  topology_context_id  BIGINT        NOT NULL,

  -- The actual recommendation made by the market.
  -- Changes are stored as raw protobuf written to disk.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/action/ActionDTO.proto
  recommendation       BLOB          NOT NULL,

  recommendation_time  TIMESTAMP     NOT NULL,

  action_decision      BLOB          NOT NULL,

  execution_step       BLOB          NOT NULL,

  current_state        INT           NOT NULL,

  user_name            VARCHAR(255)  NOT NULL,

  PRIMARY KEY (id)
);

-- Add an index to make finding market actions by their action plan or topology context
CREATE INDEX action_history_id_idx ON action_history (id);
CREATE INDEX action_history_topology_context_id_idx ON action_history (topology_context_id);