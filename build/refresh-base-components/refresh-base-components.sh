#!/bin/bash -e

COMPONENTS=(
    nginx
    kafka
    zookeeper
    db
    arangodb
    com.vmturbo.consul
    syslog
    vmt-component-base
    diags
)

VERSION=$1

if [[ ! $VERSION ]] ; then
    echo Please specify desired version on command line >&2
    exit 1
fi

echo Refreshing base docker components for version $VERSION

for component in ${COMPONENTS[@]}; do
    echo Pulling $component...
    if docker pull turbonomic/$component:$VERSION ; then
	echo Component $component successfully pulled
        echo
    else
	echo Failed to pull component $component >&2
        echo Abandoning further component refreshes >&2
	exit 1
    fi
done
