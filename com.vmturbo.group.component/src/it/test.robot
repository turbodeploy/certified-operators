*** Settings ***
Documentation  A resource file containing the application specific keywords
Library             Process
Library             OperatingSystem
Library             Telnet
Resource            /libDATurbonomic.resource

*** Variables ***
${Group_definition}  group_definition { \n
...  type: REGULAR \n
...  display_name: 'gRPC_Group_TESTIT_X' \n
...  is_temporary: false \n
...  static_group_members { \n
...    members_by_type { \n
...      type { \n
...        entity: 10 \n
...      } \n
...      members: 74053086960299 \n
...      members: 74053086960300 \n
...      members: 74053086960301 \n
...   } \n
...  } \n
...  } \n
...  origin { \n
...    user { \n
...    username: 'administrator' \n
...    } \n
...  }
${Properties_File}  /usr/src/project/it/resources/properties.yaml

*** Test Cases ***
TCC_001_Start_Group
    [Documentation]   Test Start Module Group
    [Tags]  Mandatory  Group

    Log to Console   ${TEST NAME}

    Inspect Infrastructure Component  db  3306
    Inspect Infrastructure Component  consul  8500
    
    Log to Console   Infrastructure OK

    Start Module1  group  com.vmturbo.group-%{PROJECT_VERSION}.jar

    ${result} =  Inspect Test Component
    IF  ${result} == False
       Fail  gRPC server not started!
    END

    ${result} =  Run Process  grpc_cli  ls  localhost:9001
    Should Be Equal As Integers  0  ${result.rc}  failed grpc_cli ls localhost:9001

    ${result}=   GRPC Call1  localhost:9001  CreateGroup  ${Group_definition}
    Log to Console   rc:${result.rc}; 
    Log to Console   stdout:${result.stdout};
    Log to Console   stderr:${result.stderr};

    # parse the response in ${result.stdout}: get GroupID
    ${groupID}=  Group ID  ${result.stdout}

    # get group to verify
    ${result}=   GRPC Call1  localhost:9001  GetGroup  id: ${groupID}
    Log to Console   ${result.rc} ${result.stdout} ${result.stderr}
    Verify Group  ${result.stdout}

    ${result}=   GRPC Call1  localhost:9001  DeleteGroup  id: ${groupID}
    Log to Console   ${result.rc} ${result.stdout} ${result.stderr}

    Terminate Module

*** Keywords ***
Inspect Infrastructure Component
    [Arguments]  ${host}  ${port}
    [Documentation]  To check the health of a compoent, we test if the component runs in docker 
    ...              and we try opening an TCP connection.
    ...                host - the hostname of the component
    ...                port - the port used by the component
    ${result} =  Run Process  docker  ps  -q  --filter  name\=${host}  --filter  status\=running
    Should not be empty  ${result.stdout}   module '${host}' not running

    ${result}=   Run Keyword And Return Status  Open Connection  ${host}  port=${port}
    FOR    ${index}    IN RANGE    5
      Exit For Loop If  ${result}
      Sleep  3s
      ${result}=   Run Keyword And Return Status  Open Connection  ${host}  port=${port}
    END
    IF  ${result} == False
      Fail  Infrastructure module ${host} not started!
    END

Inspect Test Component
    ${result}=  Wait Until Keyword Succeeds  10x  5s  Is Process Running   ${HANDLE}
    Return From Keyword If  ${result} == False  False

    Log to Console   module started, waiting for gRPC server!

    ${result}=  Wait Until Keyword Succeeds  10x  5s  GRPC Server Started
    [return]  ${result}

GRPC Server Started
    ${FileContent}=  Get file  /usr/src/project/out.txt  encoding=UTF-8
    Log to Console   waiting for gRPC server!
    ${errContent}=  Get file  /usr/src/project/err.txt  encoding=UTF-8
    Log to Console   errContent ${errContent}

    Should Contain  ${FileContent}  Initialized gRPC with services  msg=gRPC server not started!

Start Module1
    [Arguments]  ${name}  ${module}
    [Documentation]  To start a component you have to provide
    ...                name:group - the module name
    ...                module: component.jar - the jar file

    Log to Console  java -Dcomponent_type\=${name} -Dinstance_id\=${name}-1 -Dinstance_ip\=127.0.0.1 -DpropertiesYamlPath\=file:${Properties_File} -DdbRootPassword\=%{DBROOT} -classpath "/usr/src/project/${module}:/usr/src/project/deps/*" com.vmturbo.group.GroupComponent
    ${handle} =  Start Process  java
                 ...  -Dcomponent_type\=${name}  -Dinstance_id\=${name}-1  -Dinstance_ip\=127.0.0.1  -DpropertiesYamlPath\=file:${Properties_File}  -DdbRootPassword\=%{DBROOT}  -classpath  /usr/src/project/${module}:/usr/src/project/deps/*  com.vmturbo.group.GroupComponent
                 ...  cwd=/usr/src/project  stdout=out.txt  stderr=err.txt

    Set Global Variable  ${HANDLE}  ${handle}

GRPC Call1
    [Arguments]  ${address}  ${method}  ${reguest}
    Log to Console  grpc_cli call ${address} ${method} ${reguest}
    ${result} =  Run Process  grpc_cli  call  ${address}  ${method}  ${reguest}  --json_output\=true
    [return]  ${result}

Group ID
    [Arguments]  ${group_data}
    ${source data}=    Evaluate     json.loads("""${group_data}""")    json

    ${group}=    Set Variable     ${source data['group']}
    ${group_id}=   Set Variable  ${group['id']}
    [return]  ${group_id}

Create Group
    ${result}=   GRPC Call1  localhost:9001  CreateGroup  ${Group_definition}
    Log to Console   rc:${result.rc}; 
    Log to Console   stdout:${result.stdout};
    Log to Console   stderr:${result.stderr};
    Run Keyword If  '${result.rc}' == '1'  GRPC Call1  localhost:9001  DeleteGroup  display_name: 'gRPC_Group_TESTIT_1'

    ${result}=   GRPC Call1  localhost:9001  CreateGroup  ${Group_definition}
    # Should Be Equal As Integers  0  ${result.rc}
    [return]  ${result}

Verify Group
    [arguments]  ${group_data}
    ${source data}=    Evaluate     json.loads("""${group_data}""")    json

    ${group}=    Set Variable     ${source data['group']}
    ${definition}=  Set Variable  ${group['definition']}
    ${display_name}=  Set Variable  ${definition['displayName']}
    Should be equal  "${display_name}"  "gRPC_Group_TESTIT_X"
