#!/bin/bash

# logrotate for product log, called by `outchannel` in rsyslog.conf
LOG_FILE=/home/vmtsyslog/rsyslog/log.txt

# Use custom max log files if specified, otherwise 100 by default using bash parameter expansion.
MAXFILES=${LOG_MAXFILES-100}

function rotate() {
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
}
rotate ${LOG_FILE}
