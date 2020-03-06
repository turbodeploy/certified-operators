#!/bin/bash
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

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

# find the memory limit for the current environment
function find_memory_limit
{
  MEM_LIMIT=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
  echo "Memory limit is detected as $MEM_LIMIT" 2>&1 | ${LOGGER_COMMAND}
}

# We will determine the java runtime options using the following environment variables:
#
# JAVA_OPTS: If this is defined in the environment, it will be assumed to contain ALL of the
# runtime options that need to be passed to the JRE. It will be passed to the JVM as-is, and no
# other environment variables will be checked.
#
# If JAVA_OPTS is NOT defined in the environment (the default case), then we'll assemble it from
# a few constituent parts based on other environment variables, as follows:
#
#  JAVA_OPTS = JAVA_BASE_OPTS JAVA_MAX_RAM_PCT JAVA_DEBUG_OPTS (if JAVA_DEBUG is set) JAVA_ENV_OPTS JAVA_COMPONENT_OPTS
#
# JAVA_BASE_OPTS: These represent the common java runtime options we will pass to all of our java
# services. If not defined by the environment, a default set of options will be provided.
#
# JAVA_MAX_RAM_PCT: If set to a numeric value (whole number or decimal), this value will be used to
# set the -XX:MaxRAMPercentage option, which specifies the percentage of available container memory
# that can be used as Heap memory.
#
# If this variable is NOT set (default behavior), then we will choose a MaxRAMPercentage value to
# start the JVM with, based on the detected container memory limit. (see the find_memory_limit
# function in this script). If the container memory limit is < 1gb, we will set MaxRAMPercentage to
# 45.0. If the limit > 1gb, we will use 75.0
#
# If set to anything OTHER than a number (i.e. "omit"), we will interpret it as a sign that we should
# not choose this value based on container memory limit, and will not specify the -XX:MaxRAMPercentage
# setting at all.
#
# JAVA_DEBUG and JAVA_DEBUG_OPTS: Enables debugging options for the JRE, for things like opening the
# remote debugging port and enabling spring boot logging. If JAVA_DEBUG is set, then JAVA_DEBUG_OPTS
# will be appended to the command-line. Specifying JAVA_DEBUG_OPTS is optional, and a default set of
# debugging options will be added if not specified.
#
# JAVA_ENV_OPTS: Optional JVM runtime options that should be set for all JVMs (including kafka and
# zookeeper) in running in the environment.
#
# JAVA_COMPONENT_OPTS: Optional component-specific java options. If this is specified, these will be
# included in the startup command as well.
#

if [[ -z ${JAVA_OPTS} ]]; then
  # java common/base options
  if [[ -z ${JAVA_BASE_OPTS} ]]; then
     # The Djava.security.egd=file:/dev/./urandom configuration significantly speeds up start-up time
     # for the components using the SecureRandom class (see
     # http://stackoverflow.com/questions/25660899/spring-boot-actuator-application-wont-start-on-ubuntu-vps)
     JAVA_BASE_OPTS="-verbose:sizes -Xtune:virtualized -XX:+UseContainerSupport -Xms16m -XX:CompileThreshold=1500
              -Xjit:count=1500 -XX:-HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError
              -Xdump:what -Xdump:heap:none  -Xdump:java:file=/STDOUT/
              -Xdump:java:events=throw,filter=java/lang/OutOfMemoryError,file=/STDOUT/
              -XshowSettings -Djavax.xml.bind.JAXBContextFactory=com.sun.xml.bind.v2.ContextFactory
              -Djavax.xml.ws.spi.Provider=com.sun.xml.ws.spi.ProviderImpl
              -Djavax.xml.soap.SAAJMetaFactory=com.sun.xml.messaging.saaj.soap.SAAJMetaFactoryImpl
              -Djava.security.egd=file:/dev/./urandom -Djava.net.preferIPv4Stack=true
              -Dnetworkaddress.cache.ttl=0 -Dnetworkaddress.cache.negative.ttl=0"
  fi
  JAVA_OPTS=$JAVA_BASE_OPTS

  # set max ram percentage, if needed
  if [[ -n ${JAVA_MAX_RAM_PCT} ]]; then
    shopt -s extglob
    if [[ $JAVA_MAX_RAM_PCT == +([0-9])?(.+([0-9])) ]]; then
      # this is a numeric value -- use it to set the max ram pct directly
      JAVA_OPTS="$JAVA_OPTS -XX:MaxRAMPercentage=$JAVA_MAX_RAM_PCT"
    else
      # there was a non-numeric value specified, we'll assume this means we should exclude the setting.
      echo "Excluding MaxRAMPercentage option since JAVA_MAX_RAM_PCT is set to $JAVA_MAX_RAM_PCT" 2>&1 | ${LOGGER_COMMAND}
    fi
  fi

  # java dev options
  if [[ -n ${JAVA_DEBUG} ]]; then
    if [[ -z ${JAVA_DEBUG_OPTS} ]]; then
      JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,address=0.0.0.0:8000,server=y,suspend=n
               -Djdk.attach.allowAttachSelf"
    fi
    JAVA_OPTS="$JAVA_OPTS $JAVA_DEBUG_OPTS"
  fi

  # optional environment-specific options
  if [[ -n ${JAVA_ENV_OPTS} ]]; then
    JAVA_OPTS="$JAVA_OPTS $JAVA_ENV_OPTS"
  fi

  # optional component-specific options
  if [[ -n ${JAVA_COMPONENT_OPTS} ]]; then
    JAVA_OPTS="$JAVA_OPTS $JAVA_COMPONENT_OPTS"
  fi
fi

export STARTUP_COMMAND="java $JAVA_OPTS -jar $@"

# Start up the http server
/usr/bin/nohup /diags.py >/tmp/diags.log &

echo "Executing startup command: \"$STARTUP_COMMAND\"" 2>&1 | ${LOGGER_COMMAND}
exec $STARTUP_COMMAND > >($LOGGER_COMMAND) 2>&1
