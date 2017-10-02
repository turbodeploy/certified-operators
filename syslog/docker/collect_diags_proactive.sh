#!/bin/bash
rm -rf /home/vmtsyslog/tmp/rsyslog_proactive >/dev/null 2>&1
mkdir -p /home/vmtsyslog/tmp/rsyslog_proactive >/dev/null 2>&1
# Check the timestamp of the checkpoint
CHKPT=0
if [ -f /home/vmtsyslog/checkpoint ]; then
    CHKPT=$(stat --format="%Y" /home/vmtsyslog/checkpoint)
fi
# Scan all the files and remove the old ones
for logfile in $(ls /home/vmtsyslog/rsyslog/)
do
    DATESTAMP=$(stat --format="%Y" /home/vmtsyslog/rsyslog/${logfile})
    if [ ${DATESTAMP} -ge ${CHKPT} ]; then
        cp /home/vmtsyslog/rsyslog/${logfile} /home/vmtsyslog/tmp/rsyslog_proactive/
    fi
done
touch /home/vmtsyslog/checkpoint

# Zip
pushd /home/vmtsyslog/tmp >/dev/null
rm /home/vmtsyslog/tmp/rsyslog_proactive.tar.gz >/dev/null
tar cf rsyslog_proactive.tar rsyslog_proactive >/dev/null
gzip rsyslog_proactive.tar >/dev/null
rm -rf /home/vmtsyslog/tmp/rsyslog_proactive >/dev/null
popd >/dev/null
