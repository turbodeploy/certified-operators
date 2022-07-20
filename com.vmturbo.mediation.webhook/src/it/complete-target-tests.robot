*** Settings ***
Documentation   A test for module Group.
Library         OperatingSystem
Library         RequestsLibrary
Library         Collections
Library         Process
Library         /libTurboPY.py

Test Teardown       Clean Old Results


*** Test Cases ***
Probe Harness
    ${module}=  Set Variable  /usr/src/project/com.vmturbo.mediation.webhook-%{PROJECT_VERSION}.jar
    ${config}=  Set Variable  /usr/src/project/it/resources/probe-config.yml
    ${dependency}=   Set Variable  /usr/src/project/dependency
    ${actionExecutionsDTOs}=  Set Variable  /usr/src/project/it/actionExecutionsDTOs

    Is MockServer Up

    runProbeHarness  ${config}  ${module}  ${dependency}  ${actionExecutionsDTOs}

    Validate Webhook HTTP Response
    Validate Webhook HTTP Request

*** Keywords ***
Is MockServer Up
    Log to console   curl --request GET --url http://mockserver:50000/management/payload_index

    ${res2} =  Run Process  curl   http://mockserver:50000/management/payload_index
    Should Be Equal As Integers  0  ${res2.rc}  failed mockserver:50000/management/payload_index

Clean Old Results
    Remove Files  *.proto
    Remove File  WebhookProbe-action-execution.txt
    Remove Directory   __pycache__  true

Validate Webhook HTTP Request
    ${res3} =  Run Process  curl  http://mockserver:50000/management/last_payload/0
    Should Be Equal As Integers  0  ${res3.rc}  failed mockserver:50000/management/payload_index
    Log to console  ${res3.stdout}

    ${source data}=    Evaluate     json.loads("""${res3.stdout}""")  json

    ${body} =    Set Variable     ${source data['body']}
    Log to console  ${body}

    Should be equal  "Test Payload"  "${body}"

Validate Webhook HTTP Response
    ${httpStatusCode} =  Run Process  curl  -s  -o  /dev/null  -w  '\%{http_code}'  http://mockserver:50000/management/last_payload/0
    Should Be Equal As Integers  0  ${httpStatusCode.rc}  failed mockserver:50000/management/last_index/0

    Log to console  ${httpStatusCode.stdout}
    Should be equal  '200'  ${httpStatusCode.stdout}

