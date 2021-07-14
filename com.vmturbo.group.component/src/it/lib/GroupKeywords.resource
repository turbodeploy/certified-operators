*** Settings ***
Documentation   A resource file containing the Group specific keywords
Library         Process

*** Keywords ***
Start Module
    [Arguments]  ${name}  ${module}  ${setup_script}  ${suite_name}
    [Documentation]  name:group; module: target/component.jar; setup_script: src/it/resources/set_group.sh

    Run Process     /bin/bash    ${setup_script}  ${suite_name}
    ${handle} =  Start Process     java  -jar  ${module}  env:component_type=${name}  env:instance_id=${name}-1  env:instance_ip=127.0.0.1  env:propertiesYamlPath=file:../src/it/resources/properties.yaml
    Set Global Variable  ${HANDLE}  ${handle}

Inspect Group
    ${output} =  Is Process Running    ${HANDLE}
    Should Be True    ${output}

Inspect Infrastructure
    ${result} =  Run Process     /usr/bin/docker  ps  -f  name\=db
    Should Be Equal As Integers    ${result.rc}    0
    ${result} =  Run Process     /usr/bin/docker  ps  -f  name\=consul
    Should Be Equal As Integers    ${result.rc}    0

Terminate Infrastructure
    Run Keyword And Ignore Error  Run Process     docker  container  kill  db${suite_name}
    Run Keyword And Ignore Error  Run Process     docker  container  rm  db${suite_name}

    Run Keyword And Ignore Error  Run Process     docker  container  kill  consul${suite_name}
    Run Keyword And Ignore Error  Run Process     docker  container  rm  consul${suite_name}

    Run Keyword And Ignore Error  Run Process     docker  container  kill  rsyslog${suite_name}
    Run Keyword And Ignore Error  Run Process     docker  container  rm  rsyslog${suite_name}

    Run Keyword And Ignore Error  Run Process     docker  container  kill  kafka${suite_name}
    Run Keyword And Ignore Error  Run Process     docker  container  rm  kafka${suite_name}

    Run Keyword And Ignore Error  Run Process     docker  container  kill  zoo${suite_name}
    Run Keyword And Ignore Error  Run Process     docker  container  rm  zoo${suite_name}

    Run Keyword And Ignore Error  Run Process     docker  network  rm  kafka-net${suite_name}

Terminate Group
    Terminate Process    handle=${handle}
