
echo dumping full data to /tmp/vmtdb_dump.sql

mysqldump -f -u root -pvmturbo vmtdb --extended_insert=true --tables audit_log_entries classifications classifications_entities_entities ds_stats_by_day ds_stats_by_hour entities entity_assns entity_assns_members_entities entity_attrs pm_stats_by_day pm_stats_by_hour report_subscriptions standard_reports user_reports vc_licenses version_info vm_stats_by_day vm_stats_by_hour sw_stats_by_day sw_stats_by_hour da_stats_by_day da_stats_by_hour sc_stats_by_day sc_stats_by_hour > /tmp/vmtdb_dump.sql


echo dumping master data to /tmp/vmtdb_data.sql

mysqldump -f -u root -pvmturbo vmtdb --extended_insert=true --no-create-info --tables audit_log_entries classifications classifications_entities_entities entities entity_assns entity_assns_members_entities entity_attrs report_subscriptions standard_reports user_reports  > /tmp/vmtdb_data.sql


echo dumping master data schema to /tmp/vmtdb_schema.sql

mysqldump -f -u root -pvmturbo vmtdb --extended_insert=true --no-data --tables audit_log_entries classifications classifications_entities_entities entities entity_assns entity_assns_members_entities entity_attrs report_subscriptions standard_reports user_reports  > /tmp/vmtdb_schema.sql


egrep -i 'create table|engine' /tmp/vmtdb_schema.sql

cat /tmp/vmtdb_schema.sql | sed 's/InnoDB/MyISAM/g' > /tmp/vmtdb_schema_new.sql

echo differences in /tmp/vmtdb_schema_new.sql 
diff /tmp/vmtdb_schema.sql /tmp/vmtdb_schema_new.sql

echo to apply differences
echo "mysql -uroot -pvmturbo vmtdb < vmtdb_schema_new.sql"
echo "mysql -uroot -pvmturbo vmtdb < vmtdb_data.sql"
