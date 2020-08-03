/*
 Table used for normalization of hist_utilization table and reducing of character fields in it.
 It contains number to character value mapping of properties previously written in property_type
 field of stats tables, e.g. CPUAllocation, nextStepRoi, nextStepExpenses, MemAllocation and e.t.c.
 */
DROP TABLE IF EXISTS `stat_property_type`;
CREATE TABLE `stat_property_type` (
    `id` INT auto_increment,
    `property_type` VARCHAR(36),
    PRIMARY KEY(`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

/*
 Table used for normalization of hist_utilization table and reducing of character fields in it.
 It contains number to character value mapping of properties previously written in property_subtype
 field of stats tables, e.g. used, utilization, currentExpenses, nextStepRoi,
 nextStepExpenses, MemAllocation and e.t.c.
 */
DROP TABLE IF EXISTS `stat_property_subtype`;
CREATE TABLE `stat_property_subtype` (
    `id` INT auto_increment,
    `property_subtype` VARCHAR(36),
    PRIMARY KEY(`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

/*
 Table will contain statistic about historical utilization of different values described in
 stat_property_type and stat_property_subtype tables of different entities.
 */
DROP TABLE IF EXISTS `hist_utilization`;
CREATE TABLE `hist_utilization` (
    `oid` bigint REFERENCES entities(`id`),
    `producer_oid` bigint REFERENCES entities(`id`),
    `property_type_id` INT REFERENCES stat_property_type(`id`),
    `property_subtype_id` INT REFERENCES stat_property_subtype(`id`),
    `commodity_key` VARCHAR(80),
    `utilization` DECIMAL(15,3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
