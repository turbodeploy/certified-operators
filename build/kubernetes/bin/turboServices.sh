#!/bin/bash

# turboServices.sh
# Set up the kubernetes turbonomic services

# Get the parameters used for kubernetes, gluster, turbo setup
source /opt/local/etc/turbo.conf

# Set basepath for xl yaml
yamlBasePath="/opt/turbonomic/kubernetes/yaml"
logBasePath="/opt/local/logs"

# Get current image (this will have to be adjusted if we move to individual versioning
if [ x$registry == xlocalhost ]
then
  currentVersion=$(grep "image:" ${yamlBasePath}/base/api.yaml | awk -F: '{print $4}')
else
  currentVersion=$(grep "image:" ${yamlBasePath}/base/api.yaml | awk -F: '{print $3}')
fi

if [ x${currentVersion} != x${turboImage} ]
then
  find ${yamlBasePath}/ -name *.yaml | xargs sed -i "s/${currentVersion}/${turboVersion}/"
fi

# Entry Point
find ${yamlBasePath}/ -name *.yaml | xargs sed -i "s/10.0.2.15/${node}/"

# Make log dir
mkdir -p ${logBasePath}

# Production
echo ""
echo "Create Turbonomic 7 Cluster"
echo "==========================="
/opt/local/bin/turboctl create base 2>&1 ${logBasePath}/base.log
/opt/local/bin/turboctl create services 2>&1 ${logBasePath}/services.log
/opt/local/bin/turboctl create probes 2>&1 ${logBasePath}/probes.log
