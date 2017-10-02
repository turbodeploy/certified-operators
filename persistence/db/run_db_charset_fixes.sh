
# Before RPM upgrade, for pre-Aug appliances... we've seen the last step take ~20 mins on some, so
# please know that it's not hung if it takes some time.

cd /srv/rails/webapps/persistence/db

if [ -e vmtdb_fixes.sql ]; then exit; fi
if [ -e vmtdb.sql ]; then exit; fi

echo
echo "  --------------------------------------------------------------------------------"
echo "     D O   N O T   R U N   T H I S   S C R I P T   M O R E   T H A N   O N C E    "
echo "  --------------------------------------------------------------------------------"
echo


mysqldump -f -uvmtplatform -pvmturbo vmtdb --tables snapshots audit_log_entries user_reports standard_reports entity_assns classifications entities entity_attrs entity_assns_members_entities classifications_entities_entities snapshots_stats_by_hour snapshots_stats_by_day snapshots_stats_by_month snapshots_stats_utilization_range_counts_by_hour snapshots_stats_utilization_range_counts_by_day simrecords report_subscriptions > vmtdb.sql

cat vmtdb.sql | sed 's/) ENGINE=.*;/) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;/g' > vmtdb_fixes.sql


mysql -uvmtplatform -pvmturbo vmtdb < drop_views.sql

mysql -uvmtplatform -pvmturbo vmtdb < vmtdb_fixes.sql


#mysql -uvmtplatform -pvmturbo vmtdb < views.sql
#mysql -uvmtplatform -pvmturbo vmtdb < each_day.sql

#source /etc/sysconfig/persistence
#cd /srv/reports/bin
#./run_all.sh


# end #
