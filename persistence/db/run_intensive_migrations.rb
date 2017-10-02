puts "Started running intensive migrations"
@version_info_1 = VersionInfo.find(1)
if(@version_info_1.version.to_i>=45 && !VersionInfo.exists?(:id=>2))
	puts "Migrating cluster aggregation tables..."
	mig = `mysql -u root -pvmturbo vmtdb < db/migrate_intensive_2_cluster_agg_tables.sql`
	puts mig
	puts "Finished migrating cluster aggregation tables..."
end
if(@version_info_1.version.to_i>=50 && !VersionInfo.exists?(:id=>3))
  puts "Migrating cluster aggregation tables - version 50..."
  mig = `mysql -u root -pvmturbo vmtdb < db/migrate_intensive_3_cluster_agg_tables.sql`
  puts mig
  puts "Finished migrating cluster aggregation tables - version 50."
end
if(@version_info_1.version.to_i>=52 && !VersionInfo.exists?(:id=>4))
  puts "Migrating cluster aggregation tables - version 52..."
  mig = `mysql -u root -pvmturbo vmtdb < db/migrate_intensive_4_cluster_agg_tables.sql`
  puts mig
  puts "Finished migrating cluster aggregation tables - version 52."
end
if(@version_info_1.version.to_i>=53 && !VersionInfo.exists?(:id=>5))
    mig = `mysql -u root -pvmturbo vmtdb < db/migrate_intensive_5_cluster_agg_tables.sql`
    puts mig
end
puts "Finished running intensive migrations"