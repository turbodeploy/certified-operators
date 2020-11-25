#!/bin/bash

# Rotate the log before we start.
# That way, the startup sequence will only work with the current logs and
# will see the startup messages that are current.
LOG_FILE=/home/vmtsyslog/rsyslog/log.txt
if [ -f "${LOG_FILE}" ]; then
    LOG_FILE_ARCH=${LOG_FILE}_$(date +"%Y%m%d_%H%M%S")
    mv ${LOG_FILE} ${LOG_FILE_ARCH}
    # We can afford to compress the archive in background
    # in order to speed up the startup.
    /usr/bin/nohup xz ${LOG_FILE_ARCH} >/dev/null &
fi

# Start up the http server
/usr/bin/nohup /diags.py >/tmp/diags.log 2>&1 &
/usr/bin/nohup /logrotate.sh >/tmp/logrotate.log 2>&1 &
touch /home/vmtsyslog/rsyslog/log.txt
if [[ ${LOG_TO_STDOUT} != false ]]; then
  tail -F /home/vmtsyslog/rsyslog/log.txt &
fi

cp /etc/rsyslog.conf /tmp/rsyslog.conf

# Use custom logging format if specified
if [ -n "${VMTFORMAT}" ]; then
  sed -i "s/template VMTFormat.*/template VMTFormat,\"$(echo -n ${VMTFORMAT})\\\n\"/" /tmp/rsyslog.conf
fi
if [ -n "${AUDITFORMAT}" ]; then
  sed -i "s/template AuditFormat.*/template AuditFormat,\"$(echo -n ${AUDITFORMAT})\\\n\"/" /tmp/rsyslog.conf
fi
# Use custom external auditlog desctionation if specified
if [ -n "${EXTERNAL_AUDITLOG}" ]; then
  sed -i "s/remote_auditlog/${EXTERNAL_AUDITLOG}/" /tmp/rsyslog.conf
fi

rm -f /tmp/rsyslog.pid; exec /usr/sbin/rsyslogd -n -f /tmp/rsyslog.conf -i /tmp/rsyslog.pid
