#!/bin/bash -e

for i in {1..10000} ; do
    echo stdout line $i
done
echo -n "Final line without line terminator"
exit 0
