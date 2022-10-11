#!/bin/bash

if [ -z ${instance_id} ]; then
  export instance_id="kinesis-kafka-connect-1"
fi

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

# Get external config variables or use defaults
BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-kafka:9092}
CONNECTOR_NAME=${CONNECTOR_NAME:-saas-reporting-connector}
EXPORTER_TOPIC=${EXPORTER_TOPIC:-turbonomic.exporter}
KINESIS_REGION=${KINESIS_REGION:-us-east-1}
BATCH_SIZE=${BATCH_SIZE:-500}
BATCH_SIZE_BYTES=${BATCH_SIZE_BYTES:-3670016} # 3.5 MiB
KINESIS_DELIVERY_STREAM=${KINESIS_DELIVERY_STREAM:-saas-reporting-stream}

# Replace values for worker.properties
sed -i "s/\${BOOTSTRAP_SERVERS}/${BOOTSTRAP_SERVERS}/" worker.properties

# Replace values for kinesis-firehose-kafka-connect.properties
sed -i -e "s/\${CONNECTOR_NAME}/${CONNECTOR_NAME}/" \
       -e "s/\${EXPORTER_TOPIC}/${EXPORTER_TOPIC}/" \
       -e "s/\${KINESIS_REGION}/${KINESIS_REGION}/" \
       -e "s/\${BATCH_SIZE}/${BATCH_SIZE}/" \
       -e "s/\${BATCH_SIZE_BYTES}/${BATCH_SIZE_BYTES}/" \
       -e "s/\${KINESIS_DELIVERY_STREAM}/${KINESIS_DELIVERY_STREAM}/" \
       kinesis-firehose-kafka-connect.properties

# Log configuration
log_configuration() {
  echo "Starting connector with config:"
  echo "BOOTSTRAP_SERVERS:" $BOOTSTRAP_SERVERS
  echo "CONNECTOR_NAME:" $CONNECTOR_NAME
  echo "EXPORTER_TOPIC:" $EXPORTER_TOPIC
  echo "KINESIS_REGION:" $KINESIS_REGION
  echo "BATCH_SIZE:" $BATCH_SIZE
  echo "BATCH_SIZE_BYTES:" $BATCH_SIZE_BYTES
  echo "KINESIS_DELIVERY_STREAM:" $KINESIS_DELIVERY_STREAM
  echo
  echo "worker.properties:"
  cat worker.properties
  echo
  echo "kinesis-firehose-kafka-connect.properties"
  cat kinesis-firehose-kafka-connect.properties
}

log_configuration > >(${LOGGER_COMMAND}) 2>&1

# Start the connector
/usr/bin/connect-standalone worker.properties kinesis-firehose-kafka-connect.properties > >(${LOGGER_COMMAND}) 2>&1
