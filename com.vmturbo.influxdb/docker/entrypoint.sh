#!/bin/bash

export STARTUP_COMMAND="/usr/bin/influxd -config /etc/influxdb/influxdb.conf $@"

# If LOG_TO_STDOUT is defined in the environment, tee the output so that it is also logged to stdout.
# This is generally desirable in a development setup where you want to see the output on the console when
# starting a component, but not in production where we do not want logging to be captured by Docker
# and consume disk space (Docker JSON log driver captures and saves them then docker logs shows them).
# In a production environment, get the logs from the rsyslog component instead.
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

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

echo "Starting dump scheduler"
# Run via 'python -u' to prevent output buffering so that logs are immediately written.
/usr/bin/nohup python -u /influx_dump_scheduler.py > >($DUMP_LOGGER_COMMAND) 2>&1 &

echo "Executing startup command: \"$STARTUP_COMMAND\"" 2>&1 | ${LOGGER_COMMAND}
exec $STARTUP_COMMAND > >($LOGGER_COMMAND) 2>&1
