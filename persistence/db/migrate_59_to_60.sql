use vmtdb ;

start transaction ;

drop procedure if exists AddColumnUnlessExists;

delimiter '//'

create procedure AddColumnUnlessExists(
	IN dbName tinytext,
	IN tableName tinytext,
	IN fieldName tinytext,
	IN fieldDef text)
begin
	IF NOT EXISTS (
		SELECT * FROM information_schema.COLUMNS
		WHERE column_name=CONVERT(fieldName using utf8) COLLATE utf8_unicode_ci
		and table_name=CONVERT(tableName using utf8) COLLATE utf8_unicode_ci
		and table_schema=CONVERT(dbName using utf8) COLLATE utf8_unicode_ci
		)
	THEN
		set @ddl=CONCAT('ALTER TABLE ',dbName,'.',tableName,
			' ADD COLUMN ',fieldName,' ',fieldDef);
		prepare stmt from @ddl;
		execute stmt;
	END IF;
end;
//

delimiter ';'

source /srv/rails/webapps/persistence/db/IOModules.sql;
source /srv/rails/webapps/persistence/db/Chassis.sql;

call AddColumnUnlessExists('vmtdb', 'vm_stats_by_hour', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'pm_stats_by_hour', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'ds_stats_by_hour', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'vdc_stats_by_hour', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'app_stats_by_hour', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'da_stats_by_hour', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'sc_stats_by_hour', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'sw_stats_by_hour', 'commodity_key', 'VARCHAR(80)');

call AddColumnUnlessExists('vmtdb', 'vm_stats_by_day', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'pm_stats_by_day', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'ds_stats_by_day', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'vdc_stats_by_day', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'app_stats_by_day', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'da_stats_by_day', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'sc_stats_by_day', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'sw_stats_by_day', 'commodity_key', 'VARCHAR(80)');

call AddColumnUnlessExists('vmtdb', 'vm_stats_by_month', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'pm_stats_by_month', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'ds_stats_by_month', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'vdc_stats_by_month', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'app_stats_by_month', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'da_stats_by_month', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'sc_stats_by_month', 'commodity_key', 'VARCHAR(80)');
call AddColumnUnlessExists('vmtdb', 'sw_stats_by_month', 'commodity_key', 'VARCHAR(80)');

drop procedure AddColumnUnlessExists;

UPDATE vm_stats_by_hour
SET
`commodity_key` = `producer_uuid`
WHERE `property_type`='VStorage';

UPDATE vm_stats_by_day
SET
`commodity_key` = `producer_uuid`
WHERE `property_type`='VStorage';

UPDATE vm_stats_by_month
SET
`commodity_key` = `producer_uuid`
WHERE `property_type`='VStorage';


source /srv/rails/webapps/persistence/db/views.sql;
/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,60) ;

commit ;