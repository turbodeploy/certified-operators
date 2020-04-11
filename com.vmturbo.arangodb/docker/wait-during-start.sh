#!/bin/bash

# The purpose of this script is to:
#   * Wait for ArangoDB to start and
#   * In the meantime, print a message so the user knows to be patient

if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag arangodb -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag arangodb -u /tmp/log.sock)"
fi

# use while loop to check if arangodb is running
while true
do
    # Just check the arango version, to test connectivity
    arangosh --server.password root --javascript.execute-string "db._version();"
    verifier=$?
    if [ 0 = $verifier ]
         then
            echo "ArangoDB is running, $2" | $LOGGER_COMMAND
            break
         else
            echo "ArangoDB is not running yet, $1" | $LOGGER_COMMAND
            sleep 60
    fi
done
