*** Settings ***
Documentation  A resource file containing the application specific keywords
Library             Process
Library             OperatingSystem
Library             Telnet
Resource            /libDATurbonomic.resource

Suite Setup         Setup AO
Suite Teardown      Terminate Module


*** Variables ***
${Properties_File}  /usr/src/project/it/resources/properties.yaml

${CancelQueuedActions}     

${MultiActionRequest}  topology_context_id: 1 \n
...  action_ids : [637078747168364, 637078747168366] \n
...           }


*** Test Cases ***
TCC_001_Start_ActionOrchestartor
    [Documentation]   Test Start Module ActionOrchestartor
    [Tags]  Mandatory  ActionOrchestartor

    ${result} =  Run Process  grpc_cli  ls  localhost:9001  action.ActionsService
    Should Be Equal As Integers  0  ${result.rc}  failed grpc_cli ls localhost:9001
    Log to Console   rc:${result.rc}; 
    Log to Console   stdout:${result.stdout};
    Log to Console   stderr:${result.stderr};

Test_002
    [Tags]  Mandatory  ActionOrchestartor
    ${result}=   GRPC Call1  localhost:9001  action.ActionsService/CancelQueuedActions  ${CancelQueuedActions}
    Should Be Equal As Integers  0  ${result.rc}  failed grpc ls localhost:9001 ActionsService/CancelQueuedActions
    Log to Console   rc:${result.rc}; 
    Log to Console   stdout:${result.stdout};
    Log to Console   stderr:${result.stderr};

    ${source data}=    Evaluate     json.loads("""${result.stdout}""")    json

    ${cancelled}=    Set Variable     ${source data['cancelledCount']}
    Should be equal  "${cancelled}"  "0"

Test_003
    [Tags]  inprogress
    ${resizeActionfile} =  Get file  /usr/src/project/it/resources/resizeActionApiDto.json
    ${resizeActionJson}=  Evaluate  json.loads('''${resizeActionfile}''')  json

    ${result}=   GRPC Call1  localhost:9001  action.ActionsService/AcceptActions  ${resizeActionJson}
    Log to Console   rc:${result.rc}; 
    Log to Console   stdout:${result.stdout};
    Log to Console   stderr:${result.stderr};


*** Keywords ***
Setup AO
    Inspect Infrastructure Component  db  3306
    Inspect Infrastructure Component  consul  8500

    Log to Console   Infrastructure OK

    Start Module1  action-orchestrator  action-orchestrator-%{PROJECT_VERSION}.jar

    ${result} =  Inspect Test Component
    IF  ${result} == False
       Fail  gRPC server not started!
    END

Inspect Infrastructure Component
    [Arguments]  ${host}  ${port}
    [Documentation]  To check the health of a compoent, we test if the component runs in docker 
    ...              and we try opening an TCP connection.
    ...                host - the hostname of the component
    ...                port - the port used by the component
    ${result} =  Run Process  docker  ps  -q  --filter  name\=${host}  --filter  status\=running
    Should Not be Empty  ${result.stdout}   module '${host}' not running

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
    ${result}=  Wait Until Keyword Succeeds  20x  5s  Is Process Running   ${HANDLE}
    Return From Keyword If  ${result} == False  False

    Log to Console   module started, waiting for gRPC server!

    ${result}=  Wait Until Keyword Succeeds  20x  5s  GRPC Server Started
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
    ...                name: group - the module name
    ...                module: component.jar - the jar file

    Log to Console  java -Dcomponent_type\=${name} -Dinstance_id\=${name}-1 -Dinstance_ip\=127.0.0.1 -DpropertiesYamlPath\=file:${Properties_File} -DdbRootPassword\=%{DBROOT} -classpath "/usr/src/project/${module}:/usr/src/project/deps/*" com.vmturbo.action.orchestrator.ActionOrchestratorComponent
    ${handle} =  Start Process  java
                 ...  -Dcomponent_type\=${name}  -Dinstance_id\=${name}-1  -Dinstance_ip\=127.0.0.1  -DpropertiesYamlPath\=file:${Properties_File}  -DdbRootPassword\=%{DBROOT}  -classpath  /usr/src/project/${module}:/usr/src/project/deps/*  com.vmturbo.action.orchestrator.ActionOrchestratorComponent
                 ...  cwd=/usr/src/project  stdout=out.txt  stderr=err.txt

    Set Global Variable  ${HANDLE}  ${handle}

GRPC Call1
    [Arguments]  ${address}  ${method}  ${reguest}
    Log to Console  grpc_cli call ${address} ${method} ${reguest}
    ${result} =  Run Process  grpc_cli  call  ${address}  ${method}  ${reguest}  --json_output\=true
    [return]  ${result}
