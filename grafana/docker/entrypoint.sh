#!/bin/bash

# Wait until rsyslog container is up.
# If it is not, we can loose some valuable logs information related to the component startup
if [ -z "$LOG_TO_STDOUT" ]; then
    try_number=0
    while ! (echo ""> /dev/tcp/rsyslog/2514) ; do
        sleep 1;
        try_number=`expr $try_number + 1`
        echo "Waiting for rsyslog to accept connections";
        if [ "$try_number" -ge 30 ]; then
             echo "Failed to access rsyslog daemon to 30 seconds. Exiting..."
            exit 1;
        fi
    done;
    echo "Successfully reached rsyslog. Starting the component ${instance_id:-vmt_component}..."
fi

# Makes sure the first command in the piped series fails the entire thing.
set -eo pipefail

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

# If LOG_TO_STDOUT is defined in the environment, tee the output so that it is also logged to stdout.
# This is generally desirable in a development setup where you want to see the output on the console when
# starting a component, but not in production where we do not want logging to be captured by Docker
# and consume disk space (Docker JSON log driver captures and saves them then docker logs shows them).
# In a production environment, get the logs from the rsyslog component instead.
if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag ${instance_id} -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag ${instance_id} -u /tmp/log.sock)"
fi

SAAS_REPORTING=false
if [[ $(/util/config_props.py featureFlags.saasReporting) == True ]] ; then SAAS_REPORTING=true; fi

setup() {
  # figure out which reporting context we're running, defaulting to embedded-reporting
  context='embedded-reporting'
  if [[ $(/util/config_props.py deploymentMode) == SAAS ]]; then context='saas-reporting'; fi
  if $SAAS_REPORTING; then
    echo "Grafana POD executing in '$context' context"
  else
    echo "Grafana POD executing in legacy mode (SAAS_REPORTING feature flag disabled)"
  fi

  ln -s -f /grafana-plugins/* "$GF_PATHS_PLUGINS"
  ln -s -f /grafana-$context-runtime $GF_PATHS_HOME

  if $SAAS_REPORTING; then
    ln -s -f /grafana-dashboards/system-dashboards.yaml $GF_PATHS_PROVISIONING/dashboards
    ln -s -f /grafana-datasources/standard-datasources.yaml $GF_PATHS_PROVISIONING/datasources
    if [[ $context == 'saas-reporting' ]]; then
      /util/install_saas_reporting_datasources.sh
      ln -s -f /grafana-dashboards/saas-reporting-dashboards.yaml \
         $GF_PATHS_PROVISIONING/dashboards
    elif [[ $context == 'embedded-reporting' ]]; then
      /util/install_embedded_reporting_datasources.sh
      ln -s -f /grafana-dashboards/embedded-reporting-dashboards.yaml \
        $GF_PATHS_PROVISIONING/dashboards
      legacyDate="$(/util/legacy_dashboards_date.py)"
      # legacy dashboards need an as-of date added to the folder title
      if [[ $legacyDate ]] ; then
        LEGACY_DATE="$legacyDate" envsubst '$LEGACY_DATE' \
          < /grafana-dashboards/embedded-reporting-legacy-dashboards.yaml \
          > $GF_PATHS_PROVISIONING/dashboards/embedded-reporting-legacy-dashboards.yaml
      fi
      # prototype dashboards are opted-in by CR setting
      if [[ $(/util/config_props.py -c extractor,grafana includePrototypes) == True ]]; then
        ln -s -f /grafana-dashboards/embedded-reporting-prototype-dashboards.yaml \
          $GF_PATHS_PROVISIONING/dashboards
      fi
    fi
  else
    # saas-reporting feature flag disabled - don't include special plugin
    rm -f $GF_PATHS_PLUGINS/turbonomic/datacloud-grafana-datasource
  fi
}

provision_database() {
  echo "Provisioning Grafana database if needed"
  while ! /util/provision_db.py; do
    echo "Waiting 10 seconds to retry provisioning"
    sleep 10
  done
}

start_grafana() {
  echo "Starting grafana manager"
  echo "Grafana.ini file:"
  cat /etc/grafana/grafana.ini
  exec python3 /grafana_mgr.py
}

setup > >(${LOGGER_COMMAND}) 2>&1

if $SAAS_REPORTING; then
  provision_database > >(${LOGGER_COMMAND}) 2>&1
fi

start_grafana > >(${LOGGER_COMMAND}) 2>&1
