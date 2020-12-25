#!/bin/bash

# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command failed with exit code $?."' EXIT


# Upgrade non-xl application code
if [ ! -d /mnt/iso ]
then
  sudo mkdir /mnt/iso
fi
pushd /mnt/iso/
export turboVersion=$(cat version)
sudo curl -o /mnt/iso/online-packages.tar http://download.vmturbo.com/appliance/download/updates/${turboVersion}/online-packages.tar
sudo tar -xvf online-packages.tar && sudo mv online-packages/* .
/mnt/iso/turboupgrade.sh
