#!/bin/bash

# Wait until rsyslog container is up.
# It it is not, we can loose some valuable logs information related to the component startup
if [ -z "$LOG_TO_STDOUT" ]; then
    try_number=0
    while ! (echo ""> /dev/tcp/rsyslog/2514) ; do
        sleep 10;
        try_number=`expr $try_number + 1`
        echo "Waiting for rsyslog to accept connections";
        if [ "$try_number" -ge 360 ]; then
             echo "Failed to access rsyslog daemon after one hour. Exiting..."
            exit 1;
        fi
    done;
    echo "Successfully reached rsyslog. Starting the Consul component..."
fi

# If LOG_TO_STDOUT is defined in the environment, tee the output so that it is also logged to stdout.
# This is generally desirable in a development setup where you want to see the output on the console when
# starting a component, but not in production where we do not want logging to be captured by Docker
# and consume disk space (Docker JSON log driver captures and saves them then docker logs shows them).
# In a production environment, get the logs from the rsyslog component instead.
if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag consul -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag consul -u /tmp/log.sock)"
fi

set -e

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

# Note above that we run dumb-init as PID 1 in order to reap zombie processes
# as well as forward signals to all processes in its session. Normally, sh
# wouldn't do either of these functions so we'd leak zombies as well as do
# unclean termination of all our sub-processes.

# You can set CONSUL_BIND_INTERFACE to the name of the interface you'd like to
# bind to and this will look up the IP and pass the proper -bind= option along
# to Consul.
CONSUL_BIND=
if [ -n "$CONSUL_BIND_INTERFACE" ]; then
    CONSUL_BIND_ADDRESS=$(ip -o -4 addr list $CONSUL_BIND_INTERFACE | awk '{print $4}' | cut -d/ -f1)
    if [ -z "$CONSUL_BIND_ADDRESS" ]; then
        echo "Could not find IP for interface '$CONSUL_BIND_INTERFACE', exiting" 2>&1 | ${LOGGER_COMMAND}
        exit 1
    fi
    CONSUL_BIND="-bind=$CONSUL_BIND_ADDRESS"
    echo "==> Found address '$CONSUL_BIND_ADDRESS' for interface '$CONSUL_BIND_INTERFACE', setting bind option..." 2>&1 | ${LOGGER_COMMAND}
fi

# You can set CONSUL_CLIENT_INTERFACE to the name of the interface you'd like to
# bind to and this will look up the IP and pass the proper -client= option along
# to Consul.
CONSUL_CLIENT=
if [ -n "$CONSUL_CLIENT_INTERFACE" ]; then
    CONSUL_CLIENT_ADDRESS=$(ip -o -4 addr list $CONSUL_CLIENT_INTERFACE | awk '{print $4}' | cut -d/ -f1)
    if [ -z "$CONSUL_CLIENT_ADDRESS" ]; then
        echo "Could not find IP for interface '$CONSUL_CLIENT_INTERFACE', exiting" 2>&1 | ${LOGGER_COMMAND}
        exit 1
    fi
    CONSUL_CLIENT="-client=$CONSUL_CLIENT_ADDRESS"
    echo "==> Found address '$CONSUL_CLIENT_ADDRESS' for interface '$CONSUL_CLIENT_INTERFACE', setting client option..." 2>&1 | ${LOGGER_COMMAND}
fi


# This exposes three different modes, and allows for the execution of arbitrary
# commands if one of these modes isn't chosen. Each of the modes will read from
# the config directory, allowing for easy customization by placing JSON files
# there. Note that there's a common config location, as well as one specifc to
# the server and agent modes.
CONSUL_DATA_DIR=/consul/data
CONSUL_CONFIG_DIR=/consul/config
CONSUL_UI_DIR=/consul/ui

# You can also set the CONSUL_LOCAL_CONFIG environemnt variable to pass some
# Consul configuration JSON without having to bind any volumes.
if [ -n "$CONSUL_LOCAL_CONFIG" ]; then
    echo "$CONSUL_LOCAL_CONFIG" > "$CONSUL_CONFIG_DIR/local/env.json"
fi

# Remove serf config
rm -f /consul/data/serf/* >/dev/null 2>&1


###hack
# To handle changing host IP's always set up to set the 'peers' list so election will succeed
if [ -d /consul/data/raft ]
then
  echo  "[\"${CONSUL_BIND_ADDRESS}:8300\"]" > /consul/data/raft/peers.json
else
  mkdir /consul/data/raft
fi
###/hack

# The first argument is used to decide which mode we are running in. All the
# remaining arguments are passed along to Consul (or the executable if one of
# the Consul modes isn't selected).
exec > >(${LOGGER_COMMAND}) 2>&1
MODE=${1:-vmt-server}
if [ "$MODE" = 'dev' ]; then
    shift
        exec consul agent \
         -dev \
         -config-dir="$CONSUL_CONFIG_DIR/local" \
         $CONSUL_BIND \
         "$@"
elif [ "$MODE" = 'client' ]; then
    shift
        exec consul agent \
         -data-dir="$CONSUL_DATA_DIR" \
         -config-dir="$CONSUL_CONFIG_DIR/client" \
         -config-dir="$CONSUL_CONFIG_DIR/local" \
         $CONSUL_BIND \
         "$@"
elif [ "$MODE" = 'server' ]; then
    shift
        exec consul agent \
         -server \
         -data-dir="$CONSUL_DATA_DIR" \
         -config-dir="$CONSUL_CONFIG_DIR/server" \
         -config-dir="$CONSUL_CONFIG_DIR/local" \
         $CONSUL_CLIENT $CONSUL_BIND \
         "$@"
elif [ "$MODE" = 'vmt-server' ]; then
    shift
        exec consul agent \
         -bootstrap-expect 1 \
         -config-dir="$CONSUL_CONFIG_DIR/server" \
         -config-dir="$CONSUL_CONFIG_DIR/local" \
         -data-dir="$CONSUL_DATA_DIR" \
         $CONSUL_CLIENT $CONSUL_BIND \
         "$@"
elif [ "$MODE" = 'vmt-client' ]; then
    shift
        exec consul agent \
         -config-dir="$CONSUL_CONFIG_DIR/client" \
         -config-dir="$CONSUL_CONFIG_DIR/local" \
         -data-dir="$CONSUL_DATA_DIR" \
         $CONSUL_CLIENT $CONSUL_BIND \
         "$@"
else
    exec "$@"
fi

