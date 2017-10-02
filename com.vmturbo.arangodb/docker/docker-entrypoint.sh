#!/bin/bash
# Makes sure the first command in the piped series fails the entire thing.
set -eo pipefail

# rsyslog
/usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

# Arango dump and restore service
/usr/bin/nohup /arango_dump_restore.py >/tmp/arango_dump_restore.log &

# if command starts with an option, prepend arangod
if [ "${1:0:1}" = '-' ]; then
	set -- arangod "$@"
fi

# Remove the lock. We will not be running multiple instances on the same Docker host
if [[ -f "/var/lib/arangodb3/LOCK" ]]; then
    echo "Warning: Removing the lock" 2>&1 | logger --tag arangodb -u /tmp/log.sock
    rm -f /var/lib/arangodb3/LOCK
fi

if [ "$1" = 'arangod' ]; then
	if [ -f /tmp/init_007 ]; then
	    rm -f /tmp/init_007
        echo "Initializing database...Hang on..." 2>&1 | logger --tag arangodb -u /tmp/log.sock
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
			    echo "ArangoDB didn't start correctly during init" 2>&1 | logger --tag arangodb -u /tmp/log.sock
			    cat /tmp/init-log
			    exit 1
		    fi
		        let counter=counter+1
		    ARANGO_UP=1
            echo "db._version()" | arangosh --server.endpoint=unix:///tmp/arangodb-tmp.sock 2>&1 > /dev/null || ARANGO_UP=0
		done

		if ! kill -s TERM "$pid" || ! wait "$pid"; then
            echo 'ArangoDB Init failed.' 2>&1 | logger --tag arangodb -u /tmp/log.sock
            exit 1
		fi

        echo "Database initialized...Starting System..." 2>&1 | logger --tag arangodb -u /tmp/log.sock
	fi

	# if we really want to start arangod and not bash or any other thing
	# prepend --authentication as the FIRST argument
	# (so it is overridable via command line as well)
	shift

	set -- arangod --server.authentication="true" "$@"
fi

exec "$@" 2>&1 | logger --tag arangodb -u /tmp/log.sock
