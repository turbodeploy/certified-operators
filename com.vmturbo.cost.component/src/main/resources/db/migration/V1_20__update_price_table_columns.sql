DROP TABLE IF EXISTS price_table_key_oid;

-- persist OIDs for discovered price_table_key here.
CREATE TABLE `price_table_key_oid`(

  -- unique ID for this price_table_key
  `id` BIGINT NOT NULL,

  `price_table_key` LONGTEXT NOT NULL,

  -- the unique ID for this price table
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;


TRUNCATE TABLE price_table;
ALTER TABLE price_table DROP COLUMN ASSOCIATED_PROBE_TYPE;
ALTER TABLE price_table ADD COLUMN oid BIGINT NOT NULL PRIMARY KEY;
ALTER TABLE price_table ADD COLUMN price_table_key LONGTEXT;
ALTER TABLE price_table ADD COLUMN checksum BIGINT;
ALTER TABLE price_table ADD FOREIGN KEY (oid) REFERENCES price_table_key_oid(id) ON DELETE CASCADE;
