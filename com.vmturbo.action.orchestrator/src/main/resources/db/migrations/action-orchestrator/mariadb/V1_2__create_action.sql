-- A market action consists of the recommendation produced by a market analysis.
-- A market action is immutable.
-- It does not contain the mutable state associated with actually executing an action.
-- Market actions are grouped together into an action plan.
CREATE TABLE market_action (
  -- The id of the action.
  id                   BIGINT        NOT NULL,

  -- The id of the parent action plan.
  action_plan_id       BIGINT        NOT NULL,

  -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
  topology_context_id  BIGINT        NOT NULL,

  -- The actual recommendation made by the market.
  -- Changes are stored as raw protobuf written to disk.
  -- See com.vmturbo.common.protobuf/src/main/protobuf/action/ActionDTO.proto
  recommendation       BLOB          NOT NULL,

  PRIMARY KEY (id),
  CONSTRAINT fk_action_plan_id FOREIGN KEY (action_plan_id) REFERENCES action_plan(id) ON DELETE CASCADE
);

-- Add an index to make finding market actions by their action plan or topology context
CREATE INDEX market_action_plan_id_idx ON market_action (action_plan_id);
CREATE INDEX market_action_topology_context_id_idx ON market_action (topology_context_id);