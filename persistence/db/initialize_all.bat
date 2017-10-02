mysql -u root -pvmturbo < create.mysql.sql
mysql -u root -pvmturbo < grants.sql
mysql -u root -pvmturbo vmtdb < summary_tables.sql
mysql -u root -pvmturbo vmtdb < add_indices.sql
mysql -u root -pvmturbo vmtdb < views.sql
mysql -u root -pvmturbo vmtdb < add_ref_data.sql
