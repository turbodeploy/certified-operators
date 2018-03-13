## FIND PID OF THIS PROCESS
ppid () { ps -p ${1:-$$} -o ppid=; }

## FIND THE AVAILABLE COMPRESSION METHOD
if command -v xz >/dev/null; then 
	COMPRESSOR=xz
	FILE_EXT=xz
else 
	COMPRESSOR=gzip
	FILE_EXT=gz
fi

cd $persistence_BASE
	#Check that no other RUN_ALL is currently running:
	RUN_ALL=`pgrep -f run_all`
	#echo $RUN_ALL >> log/reports.log
	MY_PPID=`ppid`
	#echo $PPID >> log/reports.log
	if pgrep -f run_all > /dev/null; then
		for p in $RUN_ALL; do
			if [ $p -ne $$ ] && [ $p -ne $MY_PPID ] && [ $p -lt $$ ]; then
				echo "Exiting: Another run_all pid: $p is already running while my pid: $$ and my ppid: $MY_PPID" >> log/reports.log 2>&1
				exit
			fi
		done
	fi
	    
	#If the appliance is an Aggregator then exit 
    isAggregator=$(curl -s "http://guest:guest@localhost/vmturbo/api?inv.c&ServiceProxyManager&isAggregationRequired")

	if [ "$isAggregator" == "true" ]
 	 then
        echo "Exiting: reporting daily job as it is an aggregator." > log/reports.log 2>&1
        exit
	fi
	
	#This is the only script - continue with the normal jobs:
	
	## COMPRESS OLD REPORTS LOG:
	$COMPRESSOR -c log/reports.log > log/reports.`date +"%Y-%m-%d"`.log.$FILE_EXT
	
	echo Daily job started: `date` > log/reports.log 2>&1
	
	#echo running resource intensive migrations... `date` >> log/reports.log 2>&1
	#ruby ./script/runner -e production db/run_intensive_migrations.rb >> log/reports.log 2>&1
        
	#echo refreshing group info from platform... `date` >> log/reports.log 2>&1
	#ruby ./script/runner -e production script/refresh_group_info.rb >> log/reports.log 2>&1

	#echo running each_day.sql data population sql script... `date` >> log/reports.log 2>&1
	### #mysql -uvmtplatform -pvmturbo < $persistence_BASE/db/each_day.sql >> log/reports.log 2>&1
	###tsql -U vmtplatform -P vmturbo -S $(SERVER) -D vmtdb -o q < $persistence_BASE/db/SQLServer/each_day.mssql.sql >> log/reports.log 2>&1

	#ruby ./script/runner -e production script/make_daily_make_all.rb > $VMT_REPORTS_HOME/bin/daily_make_all.sh
	#chmod +x $VMT_REPORTS_HOME/bin/daily_make_all.sh

	#echo running reports...  `date` >> log/reports.log 2>&1
	#$VMT_REPORTS_HOME/bin/daily_make_all.sh >> log/reports.log 2>&1

	#echo sending reports to email subscribers...  `date` >> log/reports.log 2>&1
	#ruby ./script/runner -e production script/email_reports.rb >> log/reports.log 2>&1
	
	echo `date` >> /srv/rails/webapps/persistence/log/disk_mysql.txt
	du -h `echo "/var/lib/mysql" | cut -d- -f2 | cut -d. -f2` >> /srv/rails/webapps/persistence/log/disk_mysql.txt
	ls -lh `echo "/var/lib/mysql/vmtdb" | cut -d- -f2 | cut -d. -f2` >> /srv/rails/webapps/persistence/log/disk_mysql.txt
	echo "==================================" >> /srv/rails/webapps/persistence/log/disk_mysql.txt
	
    echo zipping reports...  `date` >> log/reports.log 2>&1
    gzip $VMT_REPORTS_HOME/pdf_files/20*/*.pdf > /tmp/ziplog.log 2>&1
    gzip $VMT_REPORTS_HOME/pdf_files/20*/*.xls >> /tmp/ziplog.log 2>&1
    if [ -f /tmp/ziplog.log ]; then
        rm /tmp/ziplog.log
    fi

## DELETE OLD ON-DEMAND REPORTS
rm /srv/reports/pdf_files/on_demand/*.pdf

## DELETE OLD LOG FILES
echo "Deleting log files older than 30 days..." `date` >> log/reports.log 2>&1
find log/reports.*.$FILE_EXT -mtime +30 -exec rm {} \;

echo Daily job done: `date`
