
mysql -u root -pvmturbo vmtdb < utils/drop_data.sql
ruby script/runner -e production script/refresh_group_info.rb
ruby script/runner -e production script/synthesize_by_hour_data.rb
mysql -u root -pvmturbo vmtdb < db/each_day.sql
