#!/bin/bash -e

# this script sleeps for 30 seconds and then exits. It does not trap
# any signals, so the timeout TERM signal sent by the executor should
# cause it to exit immediately, i.e. within the grace period.
#
# Note that bash defers signal delivery when there's a foreground
# process, until after that process exits. Thus for this test
# we exec sleep rather than just runnnig it in as a subprocess.

exec $(which sleep) 30

