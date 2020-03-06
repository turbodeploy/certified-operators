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

	    # Test arangoDB version for a mismatch (search for old 3.3.22 version from previous image)
	  echo "Testing if a database upgrade is needed." | $LOGGER_COMMAND
    if [[ `grep  '30323' -r  /var/lib/arangodb3/databases/**/VERSION 2>/dev/null | wc -l` -gt 0 ]] ; then
        echo "Database upgrade needed!" | $LOGGER_COMMAND
        UPGRADE_FLAGS="--database.auto-upgrade true"
        # Replace config file on upgrade
        echo "Copying default arangodb config file from $DEFAULT_ARANGO_CONF to $ARANGO_CONF as part of the upgrade" | $LOGGER_COMMAND
        cp $DEFAULT_ARANGO_CONF $ARANGO_CONF 2>&1 | $LOGGER_COMMAND
        echo "ArangoDB will upgrade itself from version 3.3.23 to version 3.6.1." | $LOGGER_COMMAND
        echo "This may take a long time, depending on the amount of existing data." | $LOGGER_COMMAND
        /wait-during-start.sh "the upgrade is still in progress. Please be patient, this may take some time." "the  upgrade is complete!" &
    # Test if an existing mmfiles database needs to be replaced with rocksDB
    elif [[ `grep 'mmfiles'  /var/lib/arangodb3/ENGINE 2>/dev/null | wc -l` -gt 0 ]] ; then
        # Location for the mmfiles ArangoDB database backup
        MMFILES_DUMP_LOCATION=/var/lib/arangodb3/mmfiles-dump
        if [[ ! -d  "$MMFILES_DUMP_LOCATION" ]] ; then
            # Run mmfiles-dump in background waiting for ArangoDB to start
            /mmfiles-dump.sh &
        else
            echo "Database backup was successfully stored at $MMFILES_DUMP_LOCATION. Deleting the obsolete mmfiles data files..." | $LOGGER_COMMAND
            rm -rf /var/lib/arangodb3/journals | $LOGGER_COMMAND
            rm -rf /var/lib/arangodb3/databases | $LOGGER_COMMAND
            rm -rf /var/lib/arangodb3/ENGINE | $LOGGER_COMMAND
            # Run mmfiles-restore in background waiting for ArangoDB to start
            /mmfiles-restore.sh &
        fi
    fi

	# if we really want to start arangod and not bash or any other thing
	# prepend --authentication as the FIRST argument
	# (so it is overridable via command line as well)
	shift
	set -- arangod --server.authentication="true" $UPGRADE_FLAGS --configuration $ARANGO_CONF "$@"
fi

echo "Starting arango with these arguments: $@" | $LOGGER_COMMAND
exec "$@" > >($LOGGER_COMMAND) 2>&1
