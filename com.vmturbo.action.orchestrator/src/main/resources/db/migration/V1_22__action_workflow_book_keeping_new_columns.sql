ALTER TABLE action_workflow_book_keeping
  -- the target entity of the action
  ADD target_entity_id BIGINT NOT NULL,
  -- The name of the policy setting that the workflow was applied to on the target_entity_id.
  -- The longest setting at time of adding this column was 57 characters.
  ADD setting_name varchar(255) NOT NULL;