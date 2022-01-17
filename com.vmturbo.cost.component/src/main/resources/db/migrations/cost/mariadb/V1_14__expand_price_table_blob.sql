-- The "BLOB" datatype can fit objects up to 64kb.
-- This is not always big enough for the price tables, so we migrate
-- them to MEDIUMBLOB which goes up to 16MB.

ALTER TABLE price_table MODIFY price_table_data MEDIUMBLOB NOT NULL;

ALTER TABLE price_table MODIFY ri_price_table_data MEDIUMBLOB NOT NULL;