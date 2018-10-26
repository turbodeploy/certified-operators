#!/bin/bash

# Makes sure the first command in the piped series fails the entire thing.
set -eo pipefail

echo "Setting up rsyslog"

# rsyslog
/usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

echo "exporting logger command"

if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag influxdb -u /tmp/log.sock"
  export DUMP_LOGGER_COMMAND="logger --tag influxdb-dump -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag influxdb -u /tmp/log.sock)"
  export DUMP_LOGGER_COMMAND="eval tee >(logger --tag influxdb-dump -u /tmp/log.sock)"
fi

# Check if METRON is enabled. if not, immediately exit.
if [[ "${METRON_ENABLED}" != "true" ]]; then
  echo "Metron is not enabled. Not starting InfluxDB." > >($LOGGER_COMMAND) 2>&1
  exit 0
fi

echo "exporting startup command"
export STARTUP_COMMAND="influxd -config /etc/influxdb/influxdb.conf $@"

LOCATION=$(which influxd)
echo "Found influxd at ${LOCATION}"

echo "Starting dump scheduler"

# Run via 'python -u' to prevent output buffering so that logs are immediately written.
/usr/bin/nohup python -u /influx_dump_scheduler.py > >($DUMP_LOGGER_COMMAND) 2>&1 &

echo "Executing startup command: \"$STARTUP_COMMAND\"" 2>&1 | ${LOGGER_COMMAND}
exec $STARTUP_COMMAND > >($LOGGER_COMMAND) 2>&1
