-- Container settings are migrated to ContainerSpec. No migration path is provided for
-- existing container settings because they were not widely used and also because we have
-- dramatically changed the model and appropriate settings for containers and instead
-- of migrating existing settings we encourage customers to evaluate the appropriate
-- settings with the new model in mind.
DELETE FROM setting_policy WHERE entity_type = 40;