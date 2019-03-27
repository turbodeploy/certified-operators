#!/bin/bash -e

for i in {1..10000} ; do
    echo stdout line $i
    echo stderr line $i >&2
done
exit 0
