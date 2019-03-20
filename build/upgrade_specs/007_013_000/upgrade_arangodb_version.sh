#!/bin/bash

set -e

# Upgrading from existing 3.1.21 to 3.3.22 involves running arangodb first in
# the upgrade mode, verifying things are ok and then again starting up arangodb
# in the regular mode. As these steps are complicated to run during upgrade, to
# keep things simple, we will skip the upgrade mode and just delete all the
# existing arangodb volumes.

docker stop docker_arangodb_1
docker ps -a | grep arangodb | awk '{print $1}' | xargs -r docker rm
docker volume  ls | grep arangodb | awk '{print $NF}' | xargs -r docker volume rm
