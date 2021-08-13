-- Java code has been changed to create and execute UPSERT statements directly rather than
-- using a stored proc to do that. So it's time to retire the stored proc.
DROP PROCEDURE IF EXISTS entity_savings_rollup;
