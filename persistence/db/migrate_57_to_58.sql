use vmtdb ;

start transaction ;

drop procedure if exists AddColumnUnlessExists;
drop procedure if exists delIndex;

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
			' ADD COLUMN ',fieldName,' ',fieldDef,' DEFAULT -1');
		prepare stmt from @ddl;
		execute stmt;
	END IF;
end;
//

create procedure delIndex(
	IN indexName VARCHAR(128),
	IN tableName VARCHAR(128)
)
BEGIN
 IF((SELECT COUNT(*) AS index_exists 
FROM information_schema.statistics 
WHERE table_schema='vmtdb'
AND table_name=CONVERT(tableName using utf8) COLLATE utf8_unicode_ci
AND index_name =CONVERT(indexName using utf8) COLLATE utf8_unicode_ci ) > 0) 
THEN
   SET @s = CONCAT('DROP INDEX ' , indexName , ' ON ' , tableName);
   PREPARE stmt FROM @s;
   EXECUTE stmt;
 END IF;
END;
//

delimiter ';'

call delIndex('sw_bh_uuid_idx' , 'sw_stats_by_hour');
call delIndex('sw_bh_property_type_idx' , 'sw_stats_by_hour');
call delIndex('sw_bh_property_subtype_idx' , 'sw_stats_by_hour');

call delIndex('sw_bd_snapshot_time_idx' , 'sw_stats_by_day');
call delIndex('sw_bd_uuid_idx' , 'sw_stats_by_day');
call delIndex('sw_bd_property_type_idx' , 'sw_stats_by_day');
call delIndex('sw_bd_property_subtype_idx' , 'sw_stats_by_day');

call delIndex('sw_bm_snapshot_time_idx' , 'sw_stats_by_month');
call delIndex('sw_bm_uuid_idx' , 'sw_stats_by_month');
call delIndex('sw_bm_property_type_idx' , 'sw_stats_by_month');
call delIndex('sw_bm_property_subtype_idx' , 'sw_stats_by_month');

call delIndex('da_bh_uuid_idx' , 'da_stats_by_hour');
call delIndex('da_bh_property_type_idx' , 'da_stats_by_hour');
call delIndex('da_bh_property_subtype_idx' , 'da_stats_by_hour');

call delIndex('da_bd_snapshot_time_idx' , 'da_stats_by_day');
call delIndex('da_bd_uuid_idx' , 'da_stats_by_day');
call delIndex('da_bd_property_type_idx' , 'da_stats_by_day');
call delIndex('da_bd_property_subtype_idx' , 'da_stats_by_day');

call delIndex('da_bm_snapshot_time_idx' , 'da_stats_by_month');
call delIndex('da_bm_uuid_idx' , 'da_stats_by_month');
call delIndex('da_bm_property_type_idx' , 'da_stats_by_month');
call delIndex('da_bm_property_subtype_idx' , 'da_stats_by_month');

call delIndex('sw_bh_uuid_idx' , 'sw_stats_by_hour');
call delIndex('sw_bh_property_type_idx' , 'sw_stats_by_hour');
call delIndex('sw_bh_property_subtype_idx' , 'sw_stats_by_hour');

call delIndex('sw_bd_snapshot_time_idx' , 'sw_stats_by_day');
call delIndex('sw_bd_uuid_idx' , 'sw_stats_by_day');
call delIndex('sw_bd_property_type_idx' , 'sw_stats_by_day');
call delIndex('sw_bd_property_subtype_idx' , 'sw_stats_by_day');

call delIndex('sw_bm_snapshot_time_idx' , 'sw_stats_by_month');
call delIndex('sw_bm_uuid_idx' , 'sw_stats_by_month');
call delIndex('sw_bm_property_type_idx' , 'sw_stats_by_month');
call delIndex('sw_bm_property_subtype_idx' , 'sw_stats_by_month');

call delIndex('app_bh_uuid_idx' , 'app_stats_by_hour');
call delIndex('app_bh_property_type_idx' , 'app_stats_by_hour');
call delIndex('app_bh_property_subtype_idx' , 'app_stats_by_hour');

call delIndex('app_bd_snapshot_time_idx' , 'app_stats_by_day');
call delIndex('app_bd_uuid_idx' , 'app_stats_by_day');
call delIndex('app_bd_property_type_idx' , 'app_stats_by_day');
call delIndex('app_bd_property_subtype_idx' , 'app_stats_by_day');

call delIndex('app_bm_snapshot_time_idx' , 'app_stats_by_month');
call delIndex('app_bm_uuid_idx' , 'app_stats_by_month');
call delIndex('app_bm_property_type_idx' , 'app_stats_by_month');
call delIndex('app_bm_property_subtype_idx' , 'app_stats_by_month');

call delIndex('vdc_bh_uuid_idx' , 'vdc_stats_by_hour');
call delIndex('vdc_bh_property_type_idx' , 'vdc_stats_by_hour');
call delIndex('vdc_bh_property_subtype_idx' , 'vdc_stats_by_hour');

call delIndex('vdc_bd_snapshot_time_idx' , 'vdc_stats_by_day');
call delIndex('vdc_bd_uuid_idx' , 'vdc_stats_by_day');
call delIndex('vdc_bd_property_type_idx' , 'vdc_stats_by_day');
call delIndex('vdc_bd_property_subtype_idx' , 'vdc_stats_by_day');

call delIndex('vdc_bm_snapshot_time_idx' , 'vdc_stats_by_month');
call delIndex('vdc_bm_uuid_idx' , 'vdc_stats_by_month');
call delIndex('vdc_bm_property_type_idx' , 'vdc_stats_by_month');
call delIndex('vdc_bm_property_subtype_idx' , 'vdc_stats_by_month');

call delIndex('cl_bd_recorded_on_idx' , 'cluster_stats_by_day');
call delIndex('cl_bd_internal_name_idx' , 'cluster_stats_by_day');
call delIndex('cl_bd_property_type_idx' , 'cluster_stats_by_day');
call delIndex('cl_bd_property_subtype_idx' , 'cluster_stats_by_day');

call delIndex('cl_bm_recorded_on_idx' , 'cluster_stats_by_month');
call delIndex('cl_bm_internal_name_idx' , 'cluster_stats_by_month');
call delIndex('cl_bm_property_type_idx' , 'cluster_stats_by_month');
call delIndex('cl_bm_property_subtype_idx' , 'cluster_stats_by_month');

call delIndex('cl_m_recorded_on_idx' , 'cluster_members');
call delIndex('cl_m_internal_name_idx' , 'cluster_members');
call delIndex('cl_m_group_uuid_idx' , 'cluster_members');
call delIndex('cl_m_member_uuid_idx' , 'cluster_members');
call delIndex('cl_m_group_type_idx' , 'cluster_members');

drop procedure delIndex;

source /srv/rails/webapps/persistence/db/Disk_Array.sql;
source /srv/rails/webapps/persistence/db/Storage_Controller.sql;
source /srv/rails/webapps/persistence/db/Switch.sql;

source /srv/rails/webapps/persistence/db/Switch_indices.sql; 
source /srv/rails/webapps/persistence/db/Disk_Array_indices.sql; 
source /srv/rails/webapps/persistence/db/Storage_Controller_indices.sql; 
source /srv/rails/webapps/persistence/db/indices_for_old_tables.sql; 

DELETE from on_demand_reports where id=13;
INSERT INTO `on_demand_reports` VALUES (13,'VirtualMachine','Group','This report shows Virtual Machines that are over and under provisioned as indicated by their utilization of CPU and Memory','vm_group_daily_over_under_prov_grid_given_days','This report shows Virtual Machines that are over and under provisioned as indicated by their utilization of CPU and Memory  ','VM Over/Under Provisioning 90 Days','VM Group Over/Under Provisioning by Number of Days Ago');

UPDATE `standard_reports` SET title='Weekly Socket Audit Report', short_desc='Socket Audit Report Weekly', filename='weekly_socket_audit_report', period='Weekly' WHERE category='Capacity Management/Hosts' and title='Monthly Socket Audit Report';

call AddColumnUnlessExists('vmtdb', 'vm_stats_by_hour', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'pm_stats_by_hour', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'ds_stats_by_hour', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'vdc_stats_by_hour', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'app_stats_by_hour', 'relation', 'TINYINT(3)');

call AddColumnUnlessExists('vmtdb', 'vm_stats_by_day', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'pm_stats_by_day', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'ds_stats_by_day', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'vdc_stats_by_day', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'app_stats_by_day', 'relation', 'TINYINT(3)');

call AddColumnUnlessExists('vmtdb', 'vm_stats_by_month', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'pm_stats_by_month', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'ds_stats_by_month', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'vdc_stats_by_month', 'relation', 'TINYINT(3)');
call AddColumnUnlessExists('vmtdb', 'app_stats_by_month', 'relation', 'TINYINT(3)');

drop procedure AddColumnUnlessExists;

UPDATE vm_stats_by_hour
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='VCPU' or `property_type`='VMem' or `property_type`='VStorage' THEN '0'
		WHEN
			`property_type`='StorageAccess' or `property_type`='StorageLatency' or `property_type`='StorageAmount' or `property_type`='CPU' or `property_type`='IOThroughput' or `property_type`='Mem' or `property_type`='Swapping' or `property_type`='Ballooning' or `property_type`='NetThroughput' or `property_type`='Q1VCPU'  or `property_type`='Q2VCPU' or `property_type`='Q4VCPU' or `property_type`='Q8VCPU' or `property_type`='MemAllocation' or `property_type`='StorageAllocation' or `property_type`='CPUAllocation'  THEN '1'
		ELSE
			'-1'
	END
) ;
	 
UPDATE vm_stats_by_day
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='VCPU' or `property_type`='VMem' or `property_type`='VStorage' THEN '0'
		WHEN
			`property_type`='StorageAccess' or `property_type`='StorageLatency' or `property_type`='StorageAmount' or `property_type`='CPU' or `property_type`='IOThroughput' or `property_type`='Mem' or `property_type`='Swapping' or `property_type`='Ballooning' or `property_type`='NetThroughput' or `property_type`='Q1VCPU'  or `property_type`='Q2VCPU' or `property_type`='Q4VCPU' or `property_type`='Q8VCPU' or `property_type`='MemAllocation' or `property_type`='StorageAllocation' or `property_type`='CPUAllocation'  THEN '1'
		ELSE
			'-1'
	END
) ;

UPDATE vm_stats_by_month
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='VCPU' or `property_type`='VMem' or `property_type`='VStorage' THEN '0'
		WHEN
			`property_type`='StorageAccess' or `property_type`='StorageLatency' or `property_type`='StorageAmount' or `property_type`='CPU' or `property_type`='IOThroughput' or `property_type`='Mem' or `property_type`='Swapping' or `property_type`='Ballooning' or `property_type`='NetThroughput' or `property_type`='Q1VCPU'  or `property_type`='Q2VCPU' or `property_type`='Q4VCPU' or `property_type`='Q8VCPU' or `property_type`='MemAllocation' or `property_type`='StorageAllocation' or `property_type`='CPUAllocation'  THEN '1'
		ELSE
			'-1'
	END
) ;

/*PM*/
UPDATE pm_stats_by_hour
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='Swapping' or `property_type`='Ballooning' or `property_type`='CPU' or `property_type`='IOThroughput' or `property_type`='Mem' or `property_type`='NetThroughput' or `property_type`='Q1VCPU' or `property_type`='Q2VCPU' or `property_type`='Q4VCPU' or `property_type`='Q8VCPU' THEN '0'
		ELSE
			'-1'
	END
) ;
	 
UPDATE pm_stats_by_day
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='Swapping' or `property_type`='Ballooning' or `property_type`='CPU' or `property_type`='IOThroughput' or `property_type`='Mem' or `property_type`='NetThroughput' or `property_type`='Q1VCPU' or `property_type`='Q2VCPU' or `property_type`='Q4VCPU' or `property_type`='Q8VCPU' THEN '0'
		ELSE
			'-1'
	END
) ;

UPDATE pm_stats_by_month
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='Swapping' or `property_type`='Ballooning' or `property_type`='CPU' or `property_type`='IOThroughput' or `property_type`='Mem' or `property_type`='NetThroughput' or `property_type`='Q1VCPU' or `property_type`='Q2VCPU' or `property_type`='Q4VCPU' or `property_type`='Q8VCPU' THEN '0'
		ELSE
			'-1'
	END
) ;

/*DS*/
UPDATE ds_stats_by_hour
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='StorageAccess' or `property_type`='StorageLatency' or `property_type`='StorageAmount' or `property_type`='StorageProvisioned' THEN '0'
		ELSE
			'-1'
	END
) ;
	 
UPDATE ds_stats_by_day
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='StorageAccess' or `property_type`='StorageLatency' or `property_type`='StorageAmount' THEN '0'
		ELSE
			'-1'
	END
) ;

UPDATE ds_stats_by_month
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='StorageAccess' or `property_type`='StorageLatency' or `property_type`='StorageAmount' THEN '0'
		ELSE
			'-1'
	END
) ;


/*VDC*/
UPDATE vdc_stats_by_hour
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='CPUAllocation' or `property_type`='MemAllocation' or `property_type`='StorageAllocation' THEN '0'
		ELSE
			'-1'
	END
) ;
	 
UPDATE vdc_stats_by_day
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='CPUAllocation' or `property_type`='MemAllocation' or `property_type`='StorageAllocation' THEN '0'
		ELSE
			'-1'
	END
) ;

UPDATE vdc_stats_by_month
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='CPUAllocation' or `property_type`='MemAllocation' or `property_type`='StorageAllocation' THEN '0'
		ELSE
			'-1'
	END
) ;

/*App*/
UPDATE app_stats_by_hour
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='Transaction' THEN '0'
		WHEN
			`property_type`='VMem' or `property_type`='VCPU' THEN '1'
		ELSE
			'-1'
	END
) ;
	 
UPDATE app_stats_by_day
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='Transaction' THEN '0'
		WHEN
			`property_type`='VMem' or `property_type`='VCPU' THEN '1'
		ELSE
			'-1'
	END
) ;

UPDATE app_stats_by_month
SET
relation = 
(
	CASE 
		WHEN
			`property_type`='Transaction' THEN '0'
		WHEN
			`property_type`='VMem' or `property_type`='VCPU' THEN '1'
		ELSE
			'-1'
	END
) ;

source /srv/rails/webapps/persistence/db/views.sql;
/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,58) ;

commit ;