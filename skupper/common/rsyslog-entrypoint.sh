#!/bin/bash

set -o errexit

: ${COMMAND:?COMMAND environment variable not set}

log_tag=${LOG_TAG:-${HOSTNAME:-${COMPONENT_NAME:?}}}

LOGGER_COMMAND="logger --tag ${log_tag} -u /tmp/log.sock"
if [[ -n ${LOG_TO_STDOUT} ]]; then
  LOGGER_COMMAND="eval tee >($LOGGER_COMMAND)"
fi

/usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

exec $COMMAND > >($LOGGER_COMMAND) 2>&1
