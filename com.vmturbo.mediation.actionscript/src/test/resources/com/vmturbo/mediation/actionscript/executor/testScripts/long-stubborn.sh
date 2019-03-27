#!/bin/bash -e

# this script sleeps for 30 seconds and traps SIGTERM while
# it sleeps, ignoring the signal

trap '' TERM

sleep 30

