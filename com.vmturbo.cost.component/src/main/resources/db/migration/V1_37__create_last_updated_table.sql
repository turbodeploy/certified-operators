CREATE TABLE `last_updated` (
  `table_name` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `column_name` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `last_update` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
