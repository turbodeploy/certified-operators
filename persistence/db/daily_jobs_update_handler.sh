#!/bin/bash

updatehandler() {
	ACTION=$1
	MAKE_ALL=`pgrep -f daily_make_all.sh`
	
	if [ "$MAKE_ALL" != "" ]
		then
		if [ $ACTION = "stop" ] 
		then
			echo "Stopping daily report generation.."
			pkill -g $MAKE_ALL
			touch /tmp/report_killed
			echo "Stopped daily_make_all to perform an update.."
		fi
	elif [ $ACTION = "start" ] 
	then
		if [ -f /tmp/report_killed ] 
		then
			echo "Starting daily report generation.."
			rm /tmp/report_killed
			source /etc/sysconfig/persistence
			/srv/reports/bin/daily_make_all.sh >> /srv/rails/webapps/persistence/log/reports.log 2>&1 &
			echo "Started daily_make_all.."
		fi
	fi
}

if [ $# -eq 0 -o $# -gt 1 ]
then 
	echo "Usage: $(basename $0) [stop|start]"    
	exit 1
fi

updatehandler $@