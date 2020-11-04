-- Create volume_attachment_history table. For every volume attached to a vm in a cloud environment,
-- it stores volume, vm oid and last attached date. For an unattached volume, it stores the volume
-- oid with a placeholder vm oid, where the last attached date is the first date on which the volume
-- was unattached. For both cases, the last discovered date is the last date on which the volume was
-- discovered as attached with the vm or unattached.

CREATE TABLE IF NOT EXISTS `volume_attachment_history` (
  `volume_oid` bigint(20) NOT NULL,
  `vm_oid` bigint(20) NOT NULL,
  `last_attached_date` date NOT NULL,
  `last_discovered_date` date NOT NULL,
  PRIMARY KEY (`volume_oid`,`vm_oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci