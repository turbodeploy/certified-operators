#!/bin/bash

# logrotate for audit and extra logs
AUDIT_FILE=/var/log/turbonomic/audit.log
EXTRA_FILE=/home/vmtsyslog/rsyslog/extra.txt
MAXSIZE="$LOG_MAXSIZE"
MAXFILES="$LOG_MAXFILES"

if [ -z "$LOG_CHECKINTERVALSECS" ]
then
      CHECKINTERVAL=3600
else
      CHECKINTERVAL="$LOG_CHECKINTERVALSECS"
fi

function rotate() {
    # Obtain the log size
    SIZE=$(stat -c %s "$1")
    if [ ${SIZE} -gt ${MAXSIZE} ]; then
        LOG_FILE_ARCH=$1_$(date +"%Y%m%d_%H%M%S")
        cp $1 ${LOG_FILE_ARCH}
        truncate -s 0 $1
        xz ${LOG_FILE_ARCH}
        # Clean the old ones
        AMOUNT=$(ls $1* | wc -l)
        if [ ${AMOUNT} -gt ${MAXFILES} ]; then
            ls -rt1 $1* | while read -r line || [[ -n "$line" ]]
            do
                if [ ${AMOUNT} -le ${MAXFILES} ]; then
                    break
                fi
                rm ${line}
                AMOUNT=$(( AMOUNT - 1 ))
            done
        fi
    fi
}
# Check hourly for the log rotation.
while true
do
    rotate ${AUDIT_FILE}
    rotate ${EXTRA_FILE}
    sleep ${CHECKINTERVAL}
done
