-- An action plan is a set of action recommendations produced by the market after it runs an analysis.
-- It is a collection of actions associated with a specific topology and topology context.
-- Action plans are immutable and their contents are replaced when a new plan arrives for the same
-- topology context.
CREATE TABLE action_plan (
  -- The id of the action plan
  id                   BIGINT        NOT NULL,

  -- The id of the topology that the market analysed to produce this action plan.
  topology_id          BIGINT        NOT NULL,

  -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
  topology_context_id  BIGINT        NOT NULL,

  -- The time at which the action plan was created.
  create_time          TIMESTAMP     NOT NULL,

  PRIMARY KEY (id)
);