#!/bin/sh

config_dir=/usr/share/tomcat6/data/config
retention_config=${config_dir}/retention.config.topology
db_script_dir=/srv/rails/webapps/persistence/db/SQLServer
current_version=32

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

# Get current DB version
db_version=`tsql -H ${hostName} -p ${portNumber} -U vmtplatform -P vmturbo -D vmtdb -o qh << SQL
 select version from version_info; 
go
SQL` 


cd $db_script_dir
# For each version in between the db version and the current version
# run the migrate script if it is in the directory.
echo $db_version $current_version
temp_vers=$db_version
while [ $temp_vers -lt $current_version ]; do
   next_vers=`expr $temp_vers + 1` 
   script_name=migrate_${temp_vers}_to_${next_vers}.sql
   if [ -s $script_name ]; then
      tsql -H ${hostName} -p ${portNumber} -U vmtplatform -P vmturbo -D vmtdb -o q < $script_name
   fi 
   temp_vers=$next_vers
done

exit 0