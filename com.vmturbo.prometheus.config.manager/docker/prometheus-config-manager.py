#! /usr/bin/python3

# Needs to take less than 1 minute to run otherwise it will be terminated externally!

# TODO: input and output paths as well as Prometheus's reload URL should become arguments to this
# script.
import datetime
import requests
import yaml

# Read Prometheus configuration coming from Kubernetes through a config map.
prometheus_config = yaml.safe_load(open("/etc/config/prometheus.yml", "r"))

# Ask group component whether a user has enabled telemetry.
SETTINGS_ENDPOINT = "http://group:8080/SettingService/getMultipleGlobalSettings"
headers = {'Content-type': 'application/json;charset=UTF-8'}
jsonData = {'settingSpecName': ['telemetryEnabled']}
response = requests.post(url=SETTINGS_ENDPOINT, headers=headers, json=jsonData)

# Modify configuration based on the answer from group.
if not response.json()['response'][0]['booleanSettingValue']['value']:  # if not telemetry enabled
    prometheus_config['remote_write'] = []  # disable remote writer sending data to DataCloud.

# Get the customer domain and SalesForce ID from the license.
# To be implemented...

# Modify configuration to include the proper customer domain and ID.
prometheus_config['global']['external_labels']['customer_domain'] = 'tbd_customer_domain'
prometheus_config['global']['external_labels']['customer_id'] = 'tbd_customer_id'

# Write the modified configuration where Prometheus can pick it up.
yaml.safe_dump(prometheus_config, open("/etc/merged-config/prometheus.yml", "w"))

# Instruct Prometheus server to reload its configuration.
# The URL must match the one configured in Helm. And currently the '{{ .Values.server.prefixURL }}'
# part (which is currently empty) isn't taken into account. A better way would be to pass the
# URL as an argument to this script and container.
requests.post("http://prometheus-server:9090/-/reload", headers=headers)
print(datetime.datetime.now(), "INFO: Prometheus configuration updated successfully!")
