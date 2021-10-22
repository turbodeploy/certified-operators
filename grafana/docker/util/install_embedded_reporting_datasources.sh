#!/bin/bash -e

# set up substitutions and then copy the datasources provisioning file for
# embedded reporting, with substitutions, into the provisioning directory

host=$(/util/config_props.py -c extractor -db postgres:dbs.query host)
port=$(/util/config_props.py -c extractor -db postgres:dbs.query port)
database=$(/util/config_props.py -c extractor -db postgres:dbs.query databaseName)
user=$(/util/config_props.py -c extractor -db postgres:dbs.query -d query userName)
password=$(/util/config_props.py -c extractor -db postgres:dbs.query password)

HOST="$host" PORT="$port" DATABASE="$database" USER="$user" PASSWORD="$password" \
  envsubst '$HOST $PORT $DATABASE $USER $PASSWORD' \
    < /grafana-datasources/embedded-reporting-datasources.yaml \
    > $GF_PATHS_PROVISIONING/datasources/embedded-reporting-datasources.yaml
