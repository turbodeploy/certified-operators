#!/bin/bash
mkdir /home/vmtsyslog/tmp
cp --preserve=ownership,timestamps -P -R /home/vmtsyslog/rsyslog /home/vmtsyslog/tmp
pushd /home/vmtsyslog/tmp >/dev/null
rm /home/vmtsyslog/tmp/rsyslog.zip >/dev/null
zip -r rsyslog.zip rsyslog
rm -rf /home/vmtsyslog/tmp/rsyslog
popd >/dev/null
