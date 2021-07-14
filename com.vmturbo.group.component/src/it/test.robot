*** Settings ***
Documentation  A resource file containing the application specific keywords
Library             OperatingSystem
Library             Process
Resource            ${EXECDIR}${/}../src/it/lib/GroupKeywords.resource

*** Test Cases ***
TCC_001_Start_Group
    [Documentation]   Test Start Module Group
    [Tags]  Group

    Log to Console   ${TEST NAME}
    Start Module  group  com.vmturbo.group-%{PROJECT_VERSION}.jar  ../src/it/resources/set_group_support.sh  ${TEST NAME}

    Inspect Group
    Inspect Infrastructure

    Terminate Group
    Terminate Infrastructure
