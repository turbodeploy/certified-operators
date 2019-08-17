#!/bin/bash
# Makes sure the first command in the piped series fails the entire thing.
set -eo pipefail

DEFAULT_ARANGO_CONF=/etc/arangodb3/arangod.conf
ARANGO_CONF=/var/lib/arangodb3/arangod.conf

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

# Arango dump and restore service
/usr/bin/nohup /arango_dump_restore.py >/tmp/arango_dump_restore.log &

# if command starts with an option, prepend arangod
if [ "${1:0:1}" = '-' ]; then
	set -- arangod --configuration $ARANGO_CONF "$@"
fi

if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag arangodb -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag arangodb -u /tmp/log.sock)"
fi

echo "Starting Arango using custom entrypoint script..." | $LOGGER_COMMAND

# Remove the lock. We will not be running multiple instances on the same Docker host
if [[ -f "/var/lib/arangodb3/LOCK" ]]; then
    echo "Warning: Removing the lock" 2>&1 | $LOGGER_COMMAND
    rm -f /var/lib/arangodb3/LOCK
fi

if [ "$1" = 'arangod' ]; then
	# Test if arangoDB config file is missing and needs to be placed
	 if [[ ! -f $ARANGO_CONF ]] ; then
	    echo "Copying default arangodb config file from $DEFAULT_ARANGO_CONF to $ARANGO_CONF (initial setup)" | $LOGGER_COMMAND
	    cp $DEFAULT_ARANGO_CONF $ARANGO_CONF 2>&1 | $LOGGER_COMMAND
	fi

	# if we really want to start arangod and not bash or any other thing
	# prepend --authentication as the FIRST argument
	# (so it is overridable via command line as well)
	shift
	set -- arangod --server.authentication="true" $UPGRADE_FLAGS --configuration $ARANGO_CONF "$@"
fi

echo "Starting arango with these arguments: $@" | $LOGGER_COMMAND
exec "$@" > >($LOGGER_COMMAND) 2>&1
