#!/bin/sh

config_dir=/usr/share/tomcat6/data/config
retention_config=${config_dir}/retention.config.topology

hostName=`awk 'BEGIN { RS=" " } { if (substr($1,0,8) == "hostName") { print $1 } }' < $retention_config | sed 's/.*="//' | sed 's/"$//'`
portNumber=`awk 'BEGIN { RS=" " } { if (substr($1,0,9) == "portNumber") { print $1 } }' < $retention_config | sed 's/.*="//' | sed 's/"$//'`
adapter=`awk 'BEGIN { RS=" " } { if (substr($1,0,7) == "adapter") { print $1 } }' < $retention_config | sed 's/.*="//' | sed 's/".*//'`


if [ ${adapter} != "sqlserver" ]; then
echo "This is not a sqlserver db.  This script will do nothing and exit."
exit 0
fi

if [ -z ${portNumber} ]; then
  portNumber=1433
fi
echo $hostName, $portNumber, $adapter

# -- uncomment the preferred block of commands --

# -- -- -- use tsql

tsql -U vmtplatform -P vmturbo -H ${hostName} -p ${portNumber} -o q < create.mssql.sql
tsql -U vmtplatform -P vmturbo -D vmtdb -H ${hostName} -p ${portNumber} -o q < summary_tables.mssql.sql
tsql -U vmtplatform -P vmturbo -D vmtdb -H ${hostName} -p ${portNumber} -o q < add_indices.mssql.sql
tsql -U vmtplatform -P vmturbo -D vmtdb -H ${hostName} -p ${portNumber} -o q < views.mssql.sql
tsql -U vmtplatform -P vmturbo -D vmtdb -H ${hostName} -p ${portNumber} -o q < add_ref_data.mssql.sql

