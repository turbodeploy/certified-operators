#! /usr/bin/python3

import requests

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
