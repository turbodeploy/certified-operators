
-- We must always use IF NOT EXISTS to ensure every migration script is idempotent.
CREATE TABLE IF NOT EXISTS action_workflow_book_keeping (
    -- the id of the action that needs to be sent to a workflow
    action_stable_id BIGINT(20) NOT NULL,
    -- the id of the workflow that needs to receive this action
    workflow_id BIGINT(20) NOT NULL,
    -- Milliseconds since epoch since the action identified by action_stable_id has not been
    -- recommended by the market. This timestamp is used for evaluating when an action should be
    -- considered cleared.
    -- NULL cleared_timestamp means that the most recent market cycle recommended the action.
    cleared_timestamp TIMESTAMP NULL DEFAULT NULL,

    PRIMARY KEY (action_stable_id, workflow_id)
);