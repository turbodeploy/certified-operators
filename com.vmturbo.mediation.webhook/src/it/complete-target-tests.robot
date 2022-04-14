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

    Startup_Mock_Server  /usr/src/project/it/resources/protocols.yaml

    runProbeHarness  ${config}  ${module}  ${dependency}  ${actionExecutionsDTOs}

    Validate Webhook HTTP Request
    Validate Webhook HTTP Response

    Shutdown_Mock_Server 

*** Keywords ***
Clean Old Results
    Remove Files  *.proto
    Remove File  WebhookProbe-action-execution.txt
    Remove Directory   __pycache__  true

Validate Webhook HTTP Request
    ${body} =  getNthHttpRequestBody  0
    Log to console  ${body}

    ${source data}=    Evaluate     json.loads("""${body}""")  json

    ${body} =    Set Variable     ${source data['body']}
    Log to console  ${body}

    Should be equal  "Test Payload"  "${body}"

Validate Webhook HTTP Response
    ${httpStatusCode} =  getNthHttpStatusCode  0
    Log to console  ${httpStatusCode}

    Should be equal  "200"  "${httpStatusCode}"
