-- Migrate price table data from MEDIUMBLOB to LONGBLOB.

ALTER TABLE price_table MODIFY price_table_data LONGBLOB NOT NULL;

ALTER TABLE price_table MODIFY ri_price_table_data LONGBLOB NOT NULL;