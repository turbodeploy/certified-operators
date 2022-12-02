-- Migration to remove all setting policies for Application Component spec.
DELETE FROM setting_policy WHERE entity_type = 74;