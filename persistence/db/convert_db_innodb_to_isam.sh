
mysqldump -f -uvmtplatform -pvmturbo vmtdb --extended-insert=false --tables audit_log_entries classifications classifications_entities_entities entities entity_assns entity_assns_members_entities entity_attrs report_subscriptions simrecords snapshots snapshots_stats_by_day snapshots_stats_by_hour snapshots_stats_by_month snapshots_stats_utilization_range_counts_by_day snapshots_stats_utilization_range_counts_by_hour standard_reports user_reports version_info > /tmp/vmtdb_innodb.dump.sql

cat /tmp/vmtdb_innodb.dump.sql | sed 's/ENGINE=InnoDB/ENGINE=MyISAM/g' > /tmp/vmtdb_isam.dump.sql

# initialize_all.sh

# mysql -uvmtplatform -pvmturbo vmtdb < /tmp/vmtdb_isam.dump.sql

echo
echo verify (spot check) the data import - especially snapshots_stats_by_day
echo
echo bounce mysqld server
echo
echo delete the innodb ibdata1 and log files
echo   - rm /var/lib/mysql/ibdata*
echo   - rm /var/lib/mysql/ib_logfile*
echo
echo delete ./vmtdb_innodb.dump.sql
echo
