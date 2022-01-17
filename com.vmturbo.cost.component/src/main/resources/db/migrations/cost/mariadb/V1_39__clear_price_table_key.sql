-- Update the price table keys based on the service provider id.

SET FOREIGN_KEY_CHECKS=0;
Delete from price_table;
Delete from business_account_price_table_key;
Delete from price_table_key_oid;
SET FOREIGN_KEY_CHECKS=1;
