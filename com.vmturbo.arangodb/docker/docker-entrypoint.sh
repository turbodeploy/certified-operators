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
	if [ -f /tmp/init_007 ]; then
	    rm -f /tmp/init_007
        echo "Initializing database...Hang on..." 2>&1 | $LOGGER_COMMAND
        arangod --server.endpoint unix:///tmp/arangodb-tmp.sock \
                --server.authentication false \
                --log.file /tmp/init-log \
                --log.foreground-tty false &
		pid="$!"

		counter=0
		ARANGO_UP=0
		while [ "$ARANGO_UP" = "0" ];do
		    if [ $counter -gt 0 ];then
			    sleep 1
		    fi

		    if [ "$counter" -gt 100 ];then
			    echo "ArangoDB didn't start correctly during init" 2>&1 | $LOGGER_COMMAND
			    cat /tmp/init-log | $LOGGER_COMMAND
			    exit 1
		    fi
		        let counter=counter+1
		    ARANGO_UP=1
            echo "db._version()" | arangosh --server.endpoint=unix:///tmp/arangodb-tmp.sock 2>&1 > /dev/null || ARANGO_UP=0
		done

		if ! kill -s TERM "$pid" || ! wait "$pid"; then
            echo 'ArangoDB Init failed.' 2>&1 | $LOGGER_COMMAND
            exit 1
		fi

        echo "Database initialized...Starting System..." 2>&1 | $LOGGER_COMMAND
	fi

	# Test if arangoDB config file is missing and needs to be placed
	 if [[ ! -f $ARANGO_CONF ]] ; then
	    echo "Copying default arangodb config file from $DEFAULT_ARANGO_CONF to $ARANGO_CONF (initial setup)" | $LOGGER_COMMAND
	    cp $DEFAULT_ARANGO_CONF $ARANGO_CONF 2>&1 | $LOGGER_COMMAND
	fi

    # Test arangoDB version for a mismatch (search for old 3.3.22 version from previous image)
	echo "Testing if a database upgrade is needed." | $LOGGER_COMMAND
    if [[ `grep  '30322' -r  /var/lib/arangodb3/databases/**/VERSION 2>/dev/null | wc -l` -gt 0 ]] ; then
        echo "Database upgrade needed!" | $LOGGER_COMMAND
        UPGRADE_FLAGS="--database.auto-upgrade true"
        # Replace config file on upgrade
        echo "Copying default arangodb config file from $DEFAULT_ARANGO_CONF to $ARANGO_CONF as part of the upgrade" | $LOGGER_COMMAND
        cp $DEFAULT_ARANGO_CONF $ARANGO_CONF 2>&1 | $LOGGER_COMMAND
    fi

    # Test if an existing mmfiles database needs to be replaced with rocksDB
    if [[ `grep 'mmfiles'  /var/lib/arangodb3/ENGINE 2>/dev/null | wc -l` -gt 0 ]] ; then
        echo "Database using mmfiles needs to be replaced with rocksDB. Data will be lost!" | $LOGGER_COMMAND
        echo "This is a necessary, one-time conversion to ensure application stability." | $LOGGER_COMMAND
        rm -rf /var/lib/arangodb3/journals
        rm -rf /var/lib/arangodb3/databases
        rm -rf /var/lib/arangodb3/ENGINE
        # Replace config file on upgrade
        echo "Copying default arangodb config file from $DEFAULT_ARANGO_CONF to $ARANGO_CONF as part of the upgrade" | $LOGGER_COMMAND
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
