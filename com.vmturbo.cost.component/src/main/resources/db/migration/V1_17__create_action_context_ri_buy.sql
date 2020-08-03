/**
  This table stores the data displayed in the RI Buy Graph.
 */
DROP TABLE IF EXISTS `action_context_ri_buy`;
CREATE TABLE `action_context_ri_buy` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `action_id` bigint(20) DEFAULT NULL,
  `plan_id` bigint(20) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `template_type` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `template_family` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `data` varchar(5000) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;