*** Settings ***
Library                             OperatingSystem
Test Setup                          Clean Old Results

*** Variables ***
${MYFILE}  simple.txt

***Test Cases***

My Simple Robot Test
    [Tags]  Mandatory
    Create File                     simple.txt               Hello World!
    Validate my file

*** Keywords ***
Clean Old Results
    Remove File                     simple.txt

Validate my file
    File Should Exist    ${MYFILE}
    ${FileContent}=  Get file    ${MYFILE}   encoding=UTF-8
    Should Be Equal  ${FileContent}    Hello World!
