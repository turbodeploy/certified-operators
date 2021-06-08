*** Settings ***
Documentation   A test for module Group.
Library         OperatingSystem
Library         RequestsLibrary
Library         Collections
Library         Process
Library         /libPython.py

Test Setup       Clean Old Results


*** Test Cases ***
Probe Harness
    ${module}=  Set Variable  /usr/src/project/target/com.vmturbo.mediation.webhook-%{PROJECT_VERSION}.jar
    ${config}=  Set Variable  /usr/src/project/src/it/resources/probe-config.yml
    ${dependency}=   Set Variable  /usr/src/project/target/dependency
    runProbeHarness  ${config}  ${module}  ${dependency}


*** Keywords ***
Clean Old Results
    Remove Files  *.proto
    Remove Directory   __pycache__  true