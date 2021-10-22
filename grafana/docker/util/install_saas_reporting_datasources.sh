#!/bin/bash -e

# set up substitutions and then copy the datasources provisioning file for
# saas reporting, with substitutions, into the provisioning directory

project_id=$(/util/config_props.py -c grafana datacloud.projectId)
api_key=$(/util/config_props.py -c grafana datacloud.apiKey)

PROJECT_ID="$project_id" API_KEY="$api_key" \
  envsubst '$PROJECT_ID $API_KEY' \
    < /grafana-datasources/saas-reporting-datasources.yaml \
    > $GF_PATHS_PROVISIONING/datasources/saas-reporting-datasources.yaml
