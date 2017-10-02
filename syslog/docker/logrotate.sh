#!/bin/bash
LOG_FILE=/home/vmtsyslog/rsyslog/log.txt
# The max size is 100MB
MAXSIZE=104857600
MAXFILES=100
function rotate() {
    # Obtain the log size
    SIZE=$(stat -c %s "${LOG_FILE}")
    if [ ${SIZE} -gt ${MAXSIZE} ]; then
        LOG_FILE_ARCH=${LOG_FILE}_$(date +"%Y%m%d_%H%M%S")
        cp ${LOG_FILE} ${LOG_FILE_ARCH}
        truncate -s 0 ${LOG_FILE}
        xz ${LOG_FILE_ARCH}
        # Clean the old ones
        AMOUNT=$(ls ${LOG_FILE}* | wc -l)
        if [ ${AMOUNT} -gt ${MAXFILES} ]; then
            ls -rt1 ${LOG_FILE}* | while read -r line || [[ -n "$line" ]]
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
    rotate
    sleep 3600
done
