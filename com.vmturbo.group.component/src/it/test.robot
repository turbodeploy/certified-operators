*** Settings ***
Documentation  A resource file containing the application specific keywords
Library             OperatingSystem
Library             Process
Resource            /libDATurbonomic.resource

*** Variables ***
${Group_definition}  group_definition { 
...  type: REGULAR \
...  display_name: 'gRPC_Group_TESTIT_1' \
...  is_temporary: false \
...  static_group_members { \
...    members_by_type { \
...      type { \
...        entity: 10 \
...      } \
...      members: 74053086960299 \
...      members: 74053086960300 \
...      members: 74053086960301 \
...   } \
...  } \
...  } \
...  origin { \
...    user { \
...    username: 'administrator' \
...    } \
...  }
${Proto_File}  GroupDTO.proto
${DOCKER_PREFIX}  build

*** Test Cases ***
TCC_001_Start_Group
    [Documentation]   Test Start Module Group
    [Tags]  Mandatory  Group

    Log to Console   ${TEST NAME}
    Start Module  group  com.vmturbo.group-%{PROJECT_VERSION}.jar  ../src/it/resources/set_group_support.sh

    Inspect Module
    Inspect Infrastructure  db
    Inspect Infrastructure  consul
  
    Sleep	20 seconds

    ${result}=   GRPC Call  localhost:9001  CreateGroup  ${Group_definition}  ${Proto_File}
    Log to Console   ${result.rc} ${result.stdout} ${result.stderr}
    Should Be Equal As Integers  0  ${result.rc}

    # parse the response in ${result.stdout}: get GroupID
    ${groupID}=  Group ID  ${result.stdout}

    # get group to verify
    ${result}=   GRPC Call  localhost:9001  GetGroup  id: ${groupID}  ${Proto_File}
    Log to Console   ${result.rc} ${result.stdout} ${result.stderr}
    Verify Group  ${result.stdout}

    ${result}=   GRPC Call  localhost:9001  DeleteGroup  id: ${groupID}  ${Proto_File}
    Log to Console   ${result.rc} ${result.stdout} ${result.stderr}

    Terminate Module
    Terminate Infrastructure

*** Keywords ***
Group ID
    [arguments]  ${group_data}
    ${source data}=    Evaluate     json.loads("""${group_data}""")    json

    ${group}=    Set Variable     ${source data['group']}
    ${group_id}=   Set Variable  ${group['id']}
    [return]  ${group_id}

Verify Group
    [arguments]  ${group_data}
    ${source data}=    Evaluate     json.loads("""${group_data}""")    json

    ${group}=    Set Variable     ${source data['group']}
    ${definition}=  Set Variable  ${group['definition']}
    ${display_name}=  Set Variable  ${definition['displayName']}
    Should be equal  "${display_name}"  "gRPC_Group_TESTIT_1"

Terminate Infrastructure
    Run Keyword And Ignore Error  Run Process     docker  container  kill  ${DOCKER_PREFIX}_consul_1
    Run Keyword And Ignore Error  Run Process     docker  container  rm  ${DOCKER_PREFIX}_consul_1

    Run Keyword And Ignore Error  Run Process     docker  container  kill  ${DOCKER_PREFIX}_rsyslog_1
    Run Keyword And Ignore Error  Run Process     docker  container  rm  ${DOCKER_PREFIX}_rsyslog_1

    Run Keyword And Ignore Error  Run Process     docker  container  kill  ${DOCKER_PREFIX}_kafka1_1
    Run Keyword And Ignore Error  Run Process     docker  container  rm  ${DOCKER_PREFIX}_kafka1_1

    Run Keyword And Ignore Error  Run Process     docker  container  kill  ${DOCKER_PREFIX}_zoo1_1
    Run Keyword And Ignore Error  Run Process     docker  container  rm  ${DOCKER_PREFIX}_zoo1_1

    Run Keyword And Ignore Error  Run Process     docker  network  rm  ${DOCKER_PREFIX}_default
