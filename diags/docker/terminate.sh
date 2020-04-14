#!/usr/bin/env bash

# get the java pid
JAVA_PID=`pgrep java`
if [ -z "$JAVA_PID" ]; then
    echo "Java process not found, exiting."
    exit 0;
else
    echo "Sending SIGTERM To java process."

    # send SIGTERM, which should trigger a process shutdown
    kill ${JAVA_PID}

    echo "SIGTERM sent."
fi
