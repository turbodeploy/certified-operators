#!/bin/bash

# Wait until rsyslog container is up.
# If it is not, we can lose some valuable log information related to the component startup
if [ -z "$LOG_TO_STDOUT" ]; then
    try_number=0
    while ! (echo ""> /dev/tcp/rsyslog/2514) ; do
        sleep 1;
        try_number=`expr $try_number + 1`
        echo "Waiting for rsyslog to accept connections";
        if [ "$try_number" -ge 30 ]; then
             echo "Failed to access rsyslog daemon to 30 seconds. Exiting..."
            exit 1;
        fi
    done;
    echo "Successfully reached rsyslog. Starting the component ${instance_id:-vmt_component}..."
fi

# Makes sure the first command in the piped series fails the entire thing.
set -eo pipefail

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid
# If LOG_TO_STDOUT is defined in the environment, tee the output so that it is also logged to stdout.
# This is generally desirable in a development setup where you want to see the output on the console when
# starting a component, but not in production where we do not want logging to be captured by Docker
# and consume disk space (Docker JSON log driver captures and saves them then docker logs shows them).
# In a production environment, get the logs from the rsyslog component instead.
if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag ${instance_id} -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag ${instance_id} -u /tmp/log.sock)"
fi

start_hydra() {
  echo "Starting hydra"
  exec /usr/bin/hydra serve all --dangerous-force-http --config /etc/config/config.yaml
}
#start_hydra > >(${LOGGER_COMMAND}) 2>&1
start_hydra