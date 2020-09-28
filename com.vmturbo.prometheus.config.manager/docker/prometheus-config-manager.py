#! /usr/bin/python3

# This script will try hard to create some configuration for Prometheus to read, as Prometheus won't
# startup without configuration. If group or auth component isn't accessible, the last known values
# will be used or some conservative defaults if there are none.

# TODO: Input and output paths as well as Prometheus's reload URL should become arguments to this
# script.
# TODO: May need to use a proper logger and/or print stack traces.
from datetime import datetime
import re
import requests
import sys
import time
import yaml
import consul

if len(sys.argv) < 2:
    print(datetime.now(), "ERROR: Insufficient arguments! Usage: ", sys.argv[0],
          " <period-in-seconds>")
    exit(1)
period_between_runs = float(sys.argv[1])

input_config_path = "/etc/config/prometheus.yml"
output_config_path = "/etc/merged-config/prometheus.yml"
telemetry_enabled_url = "http://group:8080/SettingService/getMultipleGlobalSettings"
telemetry_labels_url = "http://auth:8080/LicenseManagerService/getLicenses"
# The URL must match the one configured in Helm. And currently the '{{ .Values.server.prefixURL }}'
# part (which is currently empty) isn't taken into account. A better way would be to pass the
# URL as an argument to this script and container.
prometheus_reload_url = "http://prometheus-server:9090/-/reload"
consul_hostname = "consul"
consul_port = "8500"

while True:
    # Read Prometheus configuration coming from Kubernetes through a config map.
    # If it's not there or it's malformed, there is no point in copying it to destination and an
    # unhandled exception is fine.
    prometheus_new_config = yaml.safe_load(open(input_config_path, "r"))

    # Attempt to read merged Prometheus configuration from a previous run. If it doesn't exist use
    # default values.
    try:
        prometheus_old_config = yaml.safe_load(open(output_config_path, "r"))
        old_remote_writers = prometheus_old_config['remote_write']
        customer_domain = prometheus_old_config['global']['external_labels']['customer_domain']
        customer_id = prometheus_old_config['global']['external_labels']['customer_id']
        instance_id = prometheus_old_config['global']['external_labels']['instance_id']
    except (yaml.YAMLError, OSError, KeyError) as error:
        print(datetime.now(), "WARNING: Can't get old values. Using defaults. Cause:", error)
        old_remote_writers = []
        customer_domain = "unlicensed"
        customer_id = "000000"
        instance_id = "00000000-0000-0000-0000-000000000000"

    try:
        try:
            # Ask group component whether a user has enabled telemetry.
            response = requests.post(url=telemetry_enabled_url,
                                     headers={'Content-type': 'application/json;charset=UTF-8'},
                                     json={'settingSpecName': ['telemetryEnabled']})

            # We want to handle unsuccessful status codes the same way as JSON parse errors and
            # connection errors.
            response.raise_for_status()

            # Modify configuration based on the response from group. If telemetry isn't enabled,
            # disable remote writer sending data to DataCloud.
            if not response.json()['response'][0]['booleanSettingValue']['value']:
                prometheus_new_config['remote_write'] = []
        except (requests.RequestException, OSError, ValueError) as error:
            print(datetime.now(),
                  "WARNING: Can't get setting from group. Using old value. Cause:", error)
            prometheus_new_config['remote_write'] = old_remote_writers

        try:
            # Get all the licenses from the auth component. We'll extract customer domain and
            # SalesForce ID from them.
            response = requests.post(url=telemetry_labels_url,
                                     headers={'Content-type': 'application/json;charset=UTF-8',
                                              'Accept': 'application/json;charset=UTF-8'},
                                     json={})

            # We want to handle unsuccessful status codes the same way as JSON parse errors and
            # connection errors.
            response.raise_for_status()

            # Modify configuration to include the proper customer domain and ID.
            # The code to get customer domain tries to mimic code to do the same thing in
            # com.vmturbo.clustermgr.DiagEnvironmentSummary.computeLicenseDomain
            customer_domain = '_'.join(sorted({
                # Remove '@' and anything before it if it exists from e-mail address to get
                # domain. Also trim whitespace.
                re.sub(r'^[^@]*@', '', license_dto['turbo']['email'].strip())
                for license_dto in response.json()['response']['licenseDTO']
                if 'turbo' in license_dto  # it's the new DTO format
                   and license_dto['turbo'] is not None  # it's a Turbonomic license
                   and 'email' in license_dto['turbo']  # it has an e-mail
                   and datetime.today() <= datetime.strptime(license_dto['turbo']['expirationDate'],
                                                             '%Y-%m-%d')  # it has not expired
            }))
            if not customer_domain:
                customer_domain = "unlicensed"
        except (requests.RequestException, OSError, ValueError) as error:
            print(datetime.now(),
                  "WARNING: Can't get licence information from auth. Using old values. Cause:",
                  error)
        finally:
            prometheus_new_config['global']['external_labels']['customer_domain'] = customer_domain
            prometheus_new_config['global']['external_labels']['customer_id'] = customer_id

        try:
            # Ask consul for the instance_id saved there by clustermgr.
            consul_client = consul.Consul(host=consul_hostname, port=consul_port)
            instance_id = consul_client.kv.get('instanceID')[1]['Value'].decode('utf-8')
        except (requests.RequestException, OSError, ValueError, consul.ConsulException) as error:
            print(datetime.now(),
                  "WARNING: Can't get instance ID from consul. Using old value. Cause:", error)
        finally:
            prometheus_new_config['global']['external_labels']['instance_id'] = instance_id
    finally:
        # Write the modified configuration where Prometheus can pick it up.
        yaml.safe_dump(prometheus_new_config, open(output_config_path, "w"))

        # Instruct Prometheus server to reload its configuration.
        requests.post(prometheus_reload_url,
                      headers={'Content-type': 'application/json;charset=UTF-8'})
        print(datetime.now(), "INFO: Prometheus configuration updated successfully!")

    time.sleep(period_between_runs)
