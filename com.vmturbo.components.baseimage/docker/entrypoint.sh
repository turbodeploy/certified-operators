#!/bin/bash

# this is a new entrypoint.sh
# The Djava.security.egd=file:/dev/./urandom configuration significantly speeds up start-up time
# for the components using the SecureRandom class (see
# http://stackoverflow.com/questions/25660899/spring-boot-actuator-application-wont-start-on-ubuntu-vps)
export STARTUP_COMMAND="java $JAVA_OPTS -jar $@"

# If LOG_TO_STDOUT is defined in the environment, tee the output so that it is also logged to stdout.
# This is generally desirable in a development setup where you want to see the output on the console when
# starting a component, but not in production where we do not want logging to be captured by Docker
# and consume disk space (Docker JSON log driver captures and saves them then docker logs shows them).
# In a production environment, get the logs from the rsyslog component instead.
if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag ${instance_id:-vmt_component} -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag ${instance_id:-vmt_component} -u /tmp/log.sock)"
fi

# Start up the http server
/usr/bin/nohup /diags.py >/tmp/diags.log &

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

echo "Executing startup command: \"$STARTUP_COMMAND\"" 2>&1 | ${LOGGER_COMMAND}
exec $STARTUP_COMMAND > >($LOGGER_COMMAND) 2>&1
