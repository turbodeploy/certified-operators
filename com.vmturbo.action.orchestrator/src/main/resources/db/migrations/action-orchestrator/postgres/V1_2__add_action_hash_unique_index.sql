-- Create a unique index on the hash table.
DROP INDEX IF EXISTS action_hash_uniq_key;
CREATE UNIQUE INDEX action_hash_uniq_key ON recommendation_identity(action_hash);