#!/bin/bash

STARTUP_COMMAND="java $SRV_JAVA_OPTS -jar $@"

# Run the Infinispan server
/opt/jboss/infinispan-server/bin/standalone.sh $ISPN_OPTS -b `hostname` &

# Run the VMT component
$STARTUP_COMMAND
