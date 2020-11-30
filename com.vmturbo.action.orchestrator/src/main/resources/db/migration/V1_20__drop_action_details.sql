-- Drop the details table as it is no longer needed
DROP TABLE IF EXISTS recommendation_identity_details;
-- Drop the details, target id, action columns
ALTER TABLE recommendation_identity
  DROP COLUMN action_details,
  DROP COLUMN target_id,
  DROP COLUMN action_type;