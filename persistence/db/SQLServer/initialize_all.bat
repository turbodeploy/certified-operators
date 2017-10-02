
rem -- uncomment the preferred block of commands --


rem -- -- -- use osql

rem osql -U vmtplatform -P vmturbo -S <server\instance> -i create.mssql.sql
rem osql -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i summary_tables.mssql.sql
rem osql -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i add_indices.mssql.sql
rem osql -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i views.mssql.sql
rem osql -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i add_ref_data.mssql.sql


rem -- -- -- use Sqlcmd

rem Sqlcmd -U vmtplatform -P vmturbo -S <server\instance> -i create.mssql.sql
rem Sqlcmd -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i summary_tables.mssql.sql
rem Sqlcmd -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i add_indices.mssql.sql
rem Sqlcmd -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i views.mssql.sql
rem Sqlcmd -U vmtplatform -P vmturbo -D vmtdb -S <server\instance> -i add_ref_data.mssql.sql
