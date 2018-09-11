#!/bin/sh

# The caller must pass in the time at which the request to perform the dump was made.
DUMP_REQUEST_TIME=${1}
if [ -z "$DUMP_REQUEST_TIME" ]; then
    echo 'Please provide the dump request timestamp. Exiting.'
    exit 1
fi

# When dumping, wait for a maximum of SLEEP_TIME_SECONDS*MAX_WAIT_ITERATIONS seconds before
# abandoning the attempt to dump the histogram.
SLEEP_TIME_SECONDS=5
MAX_WAIT_ITERATIONS=36

echo "Starting to wait for Full GC on or after timestamp $DUMP_REQUEST_TIME"

# Wait for the component to finish running a full GC and dump the object histogram.
# Because this script is called when out-of-memory, a full GC may take a long time to run.
# The object histogram is not dumped until after the full GC completes, so we need to make
# sure to wait until it is done.
COUNTER=0
while [ ${COUNTER} -lt ${MAX_WAIT_ITERATIONS} ]; do

    # Get the time of the last full GC. This time is a UNIX timestamp in milliseconds.
    # The component MemoryMonitor includes a line which has the time at which the last full garbage
    # collection completed. If this is after the time we requested the dump, we consider the histogram
    # to be available. If no full GC's have taken place, the LAST_DISCOVERY_TIME will be 0.
    LAST_FULL_GC_TIME=`python2.7 -c \
        "import urllib2; print(urllib2.urlopen('http://localhost:8080/health').read())" | \
        grep -oP 'lastFullGC \d+' | \
        awk '{print $NF}'`

    if [ ${LAST_FULL_GC_TIME} -ne 0 ] && [ ${LAST_FULL_GC_TIME} -ge ${DUMP_REQUEST_TIME} ]; then
        # A full GC occurred after we requested the dump. We can stop waiting.
        break
    fi

    COUNTER=$((COUNTER+1))
    sleep ${SLEEP_TIME_SECONDS}
    echo "Waited $COUNTER times..."
done

if [ ${COUNTER} -ge ${MAX_WAIT_ITERATIONS} ]; then
    echo "Histogram dump took too long. Abandoning after $(($COUNTER*$MAX_WAIT_ITERATIONS)) seconds."
else
    # Wait a little bit more time for the histogram to finish printing.
    sleep 1
fi