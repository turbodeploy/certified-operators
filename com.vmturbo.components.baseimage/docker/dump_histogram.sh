#!/bin/sh

# get the java pid
JAVA_PID=`pgrep java`
if [ -z "$JAVA_PID" ]; then
    echo "Java process not found, exiting."
    exit 0;
else
    echo "Requesting Histogram/thread dump."
    # send SIGQUIT that will trigger a histogram and thread dump
    kill -s QUIT $JAVA_PID
    sleep 30
    echo "Done running dump histogram script."
fi