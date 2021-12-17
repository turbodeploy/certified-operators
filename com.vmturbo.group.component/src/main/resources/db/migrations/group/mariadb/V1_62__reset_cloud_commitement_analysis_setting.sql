-- Remove old run.Cloud.Commitment.Analysis global settings 'false' value
-- It will be re-created in DefaultGlobalSettingsCreator with the new 'true' default value
DELETE FROM global_settings
WHERE name = 'run.Cloud.Commitment.Analysis';