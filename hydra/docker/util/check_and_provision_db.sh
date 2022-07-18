#!/bin/bash

echo "Provisioning Hydra database if needed"

attempts=0

while ! /util/provision_db.py; do
  echo "Waiting 10 seconds to retry provisioning"
  sleep 10
  if [ $attempts -gt 10 ]
    then
      break
  fi
  ((attempts=attempts+1))
done

if [ $1 = "migrate" ]
  then
  	echo "Running hydra auto migrate"
    export DSN=`/util/hydra_set_secrets_to_env.py dsn`
    /usr/bin/hydra migrate sql -e --yes
fi