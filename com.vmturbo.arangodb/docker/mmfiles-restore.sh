#!/bin/bash

# The purpose of this script is to:
#   * Delete the obsolete mmfiles database
#   * Wait for ArangoDB to start (with rocksDB)
#   * Restore the backup (dump) of all databases

# Location for the mmfiles ArangoDB database backup
MMFILES_DUMP_LOCATION=/var/lib/arangodb3/mmfiles-dump

if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag arangodb -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag arangodb -u /tmp/log.sock)"
fi

# Test if an mmfiles dump was taken, and now need to be restored
if [[ -d $MMFILES_DUMP_LOCATION ]] ; then
    echo "Detected mmfiles database backup stored at $MMFILES_DUMP_LOCATION. Waiting for Arango to fully start before restoring the data..." | $LOGGER_COMMAND
    # use while loop to check if arangodb is running
    while true
    do
         # Just check the arango version, to test connectivity
        arangosh --server.password root --javascript.execute-string "db._version();"
         verifier=$?
        if [ 0 = $verifier ]
            then
                 echo "ArangoDB is running, proceeding with database restore!" | $LOGGER_COMMAND
                 break
            else
                 echo "ArangoDB is not running yet, waiting to run the database restore..." | $LOGGER_COMMAND
                sleep 10
         fi
    done

    echo "Executing: arangorestore --all-databases true --create-database true --server.password root --input-directory $MMFILES_DUMP_LOCATION" | $LOGGER_COMMAND
    arangorestore --all-databases true --create-database true --server.password root --input-directory $MMFILES_DUMP_LOCATION 2>&1 | $LOGGER_COMMAND
    verifier=$?
    if [ 0 = $verifier ] ; then
         echo "arangorestore was successful. Renaming the mmfiles dump directory to avoid re-running this restore."
        mv $MMFILES_DUMP_LOCATION "$MMFILES_DUMP_LOCATION.bak"
    fi
fi