#! /usr/bin/python3

# Needs to take less than 1 minute to run!

import shutil
import requests

print("Hello prometheus config manager!")

print(os.getcwd())
print(open("/prometheus-config-manager.py", "r").read())
print(open("/etc/config/prometheus.yml", "r").read())
shutil.copy2('/etc/config/prometheus.yml', '/etc/merged-config/prometheus.yml')

# defining endpoint to get telemetryEnabled setting from group component
SETTINGS_ENDPOINT = "http://group:8080/SettingService/getMultipleGlobalSettings"
# data of POST request
jsonTelemetryData = {'settingSpecName': ['telemetryEnabled']}
# header of content type
headers = {'Content-type': 'application/json;charset=UTF-8'}

try:
    settingResponse = requests.post(url=SETTINGS_ENDPOINT, headers=headers, json=jsonTelemetryData)
    print("Response {}", str(settingResponse.json()))

    # TODO: Call Licenses API to get license information to extract domain name

    # TODO: change prometheus config based on the setting received

except requests.exceptions.ConnectionError:
    print("Caught connection error. TODO : print stacktrace")
# Instruct Prometheus server to reload its configuration.
# The URL must match the one configured in Helm. And currently the '{{ .Values.server.prefixURL }}'
# part (which is currently empty) isn't taken into account. A better way would be to pass the
# URL as an argument to this script and container.
print("Reload: ", requests.post("http://prometheus-server:9090/-/reload",
    headers=headers, json=jsonData, auth=('', '')))
