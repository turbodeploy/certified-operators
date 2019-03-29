#!/bin/bash -e

# this script sleeps for 30 seconds and traps SIGTERM while
# it sleeps, exiting with a zero exit status immediately
# if it happens

function handleTerm() {
    exit 0
}

trap handleTerm TERM

sleep 30

