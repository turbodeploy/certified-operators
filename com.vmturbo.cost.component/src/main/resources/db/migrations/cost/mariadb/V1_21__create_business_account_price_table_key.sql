DROP TABLE IF EXISTS business_account_price_table_key;

-- persist mapping of business account and price table keys .
CREATE TABLE `business_account_price_table_key`(

  -- unique ID for this business account
  `business_account_oid` BIGINT NOT NULL,

  `price_table_key_oid` BIGINT NOT NULL,

  -- the unique business account oid.
  PRIMARY KEY (`business_account_oid`),
  FOREIGN KEY (`price_table_key_oid`) REFERENCES `price_table_key_oid` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
