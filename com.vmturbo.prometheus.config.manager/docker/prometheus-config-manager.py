#! /usr/bin/python3

# Needs to take less than 1 minute to run otherwise it will be terminated externally!

# This script will try hard to create some configuration for Prometheus to read, as Prometheus won't
# startup without configuration. If group or auth component isn't accessible, the last known values
# will be used or some conservative defaults if there are none.

# TODO: Input and output paths as well as Prometheus's reload URL should become arguments to this
# script.
# TODO: May need to use a proper logger and/or print stack traces.
import datetime
import requests
import yaml

input_config_path = "/etc/config/prometheus.yml"
output_config_path = "/etc/merged-config/prometheus.yml"
telemetry_enabled_url = "http://group:8080/SettingService/getMultipleGlobalSettings"
telemetry_labels_url = ""
# The URL must match the one configured in Helm. And currently the '{{ .Values.server.prefixURL }}'
# part (which is currently empty) isn't taken into account. A better way would be to pass the
# URL as an argument to this script and container.
prometheus_reload_url = "http://prometheus-server:9090/-/reload"

# Read Prometheus configuration coming from Kubernetes through a config map.
# If it's not there or it's malformed, there is no point in copying it to destination and an
# unhandled exception is fine.
prometheus_new_config = yaml.safe_load(open(input_config_path, "r"))

# Attempt to read merged Prometheus configuration from a previews run. If it doesn't exist use
# default values.
try:
    prometheus_old_config = yaml.safe_load(open(output_config_path, "r"))
    old_remote_writers = prometheus_old_config['remote_write']
    customer_domain = prometheus_old_config['global']['external_labels']['customer_domain']
    customer_id = prometheus_old_config['global']['external_labels']['customer_id']
except (yaml.YAMLError, OSError, KeyError) as error:
    print(datetime.datetime.now(), "WARNING: Can't get old values. Using defaults. Cause:", error)
    old_remote_writers = []
    customer_domain = "unlicensed"
    customer_id = "000000"

try:
    try:
        # Ask group component whether a user has enabled telemetry.
        response = requests.post(url=telemetry_enabled_url,
                                 headers={'Content-type': 'application/json;charset=UTF-8'},
                                 json={'settingSpecName': ['telemetryEnabled']})

        # We want to handle unsuccessful status codes the same way as JSON parse errors and
        # connection errors.
        response.raise_for_status()

        # Modify configuration based on the response from group. If telemetry isn't enabled, disable
        # remote writer sending data to DataCloud.
        if not response.json()['response'][0]['booleanSettingValue']['value']:
            prometheus_new_config['remote_write'] = []
    except (requests.RequestException, OSError, ValueError) as error:
        print(datetime.datetime.now(),
              "WARNING: Can't get setting from group. Using old value. Cause:", error)
        prometheus_new_config['remote_write'] = old_remote_writers

    try:
        # Get the customer domain and SalesForce ID from the license.
        # To be implemented...

        # Modify configuration to include the proper customer domain and ID.
        customer_domain = 'tbd_customer_domain'
        customer_id = 'tbd_customer_id'
    except (requests.RequestException, OSError, ValueError) as error:
        print(datetime.datetime.now(),
              "WARNING: Can't get licence information from auth. Using old values. Cause:", error)
    finally:
        prometheus_new_config['global']['external_labels']['customer_domain'] = customer_domain
        prometheus_new_config['global']['external_labels']['customer_id'] = customer_id
finally:
    # Write the modified configuration where Prometheus can pick it up.
    yaml.safe_dump(prometheus_new_config, open(output_config_path, "w"))

    # Instruct Prometheus server to reload its configuration.
    requests.post(prometheus_reload_url, headers={'Content-type': 'application/json;charset=UTF-8'})
    print(datetime.datetime.now(), "INFO: Prometheus configuration updated successfully!")
