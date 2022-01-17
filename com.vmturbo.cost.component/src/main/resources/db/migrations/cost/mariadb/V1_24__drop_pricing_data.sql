TRUNCATE TABLE business_account_price_table_key;

-- cannot truncate due to foreign key constraint on business_account_price_table_key.
DELETE FROM price_table_key_oid;

TRUNCATE TABLE price_table;
