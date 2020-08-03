# User specific environment
if [[ -z ${JVM_TYPE} ]]; then
    JVM_TYPE="$(awk -F'"' '/JVM_TYPE=/ {print $2}' /entrypoint.sh)"
fi
if [[ "${JVM_TYPE}" == "OpenJ9" ]]; then
    export PATH=${J9PATH}
fi