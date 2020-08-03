#!/bin/bash

# The purpose of this script is to:
#   * Wait for ArangoDB to start and then
#   * Take a backup (dump) of all databases in preparation for replacing mmfiles with rocksDB

# Location for the mmfiles ArangoDB database backup
MMFILES_DUMP_LOCATION=/var/lib/arangodb3/mmfiles-dump

if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag arangodb -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag arangodb -u /tmp/log.sock)"
fi

# Test if an existing mmfiles database needs to be replaced with rocksDB
if [[ `grep 'mmfiles'  /var/lib/arangodb3/ENGINE 2>/dev/null | wc -l` -gt 0 ]] ; then
    echo "Database using mmfiles needs to be replaced with rocksDB. Data will be backed up before the upgrade." | $LOGGER_COMMAND
    echo "This is a necessary, one-time conversion to ensure application stability." | $LOGGER_COMMAND
    # use while loop to check if arangodb is running
    while true
    do
        # Just check the arango version, to test connectivity
        arangosh --server.password root --javascript.execute-string "db._version();"
        verifier=$?
        if [ 0 = $verifier ]
            then
                echo "ArangoDB is running, proceeding with mmfiles database dump!" | $LOGGER_COMMAND
                break
            else
                echo "ArangoDB is not running yet, waiting to run the mmfiles database dump..." | $LOGGER_COMMAND
                sleep 10
        fi
    done
    rm -rf $MMFILES_DUMP_LOCATION | $LOGGER_COMMAND
    echo "Executing: arangodump --all-databases true  --server.password root --output-directory $MMFILES_DUMP_LOCATION" --overwrite true | $LOGGER_COMMAND
    arangodump --all-databases true  --server.password root --output-directory "$MMFILES_DUMP_LOCATION" --overwrite true 2>&1 | $LOGGER_COMMAND
    if [[ -d $MMFILES_DUMP_LOCATION ]] ; then
        echo "Database backup was successfully stored at $MMFILES_DUMP_LOCATION. Restarting in order to upgrade to RocksDB." | $LOGGER_COMMAND
        # Restart for the upgrade
        kill 1
    fi
fi