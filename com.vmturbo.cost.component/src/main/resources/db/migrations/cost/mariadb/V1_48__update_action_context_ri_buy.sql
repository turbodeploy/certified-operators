Truncate Table action_context_ri_buy;
ALTER TABLE action_context_ri_buy MODIFY COLUMN data BLOB DEFAULT NULL;
TRUNCATE TABLE buy_reserved_instance;


