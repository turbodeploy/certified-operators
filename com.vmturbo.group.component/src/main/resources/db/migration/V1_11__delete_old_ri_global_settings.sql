-- Remove old RI global settings that have been replaced
DELETE FROM global_settings
WHERE name IN ('preferredOfferingClass', 'preferredPaymentOption','preferredTerm');