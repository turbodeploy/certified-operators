-- OM-66003: The Azure cost probe has been incorrectly returning RI specs with a Linux OS.
-- This has caused overlapping RI specs to exist in the RI spec table. In order to properly clean up
-- any incorrect RI specs, we must drop the buy_reserved_instance records that may reference them.

TRUNCATE TABLE buy_reserved_instance;