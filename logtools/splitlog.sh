#!/bin/bash -e

if ! gawk -V > /dev/null ; then
    echo "Please install GNU awk (gawk) first by running 'brew install gawk'"
    exit 1
fi

if [[ ! $1 ]] ; then echo "Reading log data from from stdin"; fi    
gawk -F '([-][0-9]+)?:' '{print > ($1 ".log")}' "$@"

$(dirname "$0")/sizelogs.sh *.log
