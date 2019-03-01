#!/bin/sh

# get the java pid
JAVA_PID=`pgrep java`
if [ -z "$JAVA_PID" ]; then
    echo "Java process not found, exiting."
    exit 0;
else
    echo "Requesting Histogram/thread dump."

    # Get the current unix timestamp in milliseconds.
    DUMP_REQUEST_TIME=`date +%s%3N`

    # send SIGQUIT that will trigger a Full GC, histogram and thread dump
    kill -s QUIT ${JAVA_PID}

    # Wait for the full GC to complete before proceeding.
    sh wait_for_full_gc.sh ${DUMP_REQUEST_TIME}

    echo "Done running dump histogram script."
fi
