#!/bin/bash

# turboServices.sh
# Set up the kubernetes turbonomic services

# Get the parameters used for kubernetes, gluster, turbo setup
source /opt/local/etc/turbo.conf

# Set basepath for xl yaml
yamlBasePath="/opt/xl/kubernetes/yaml"

# Get current image (this will have to be adjusted if we move to individual versioning
currentVersion=$(grep "image:" ${yamlBasePath}/base/api.yaml | awk -F: '{print $4}')

if [ x${currentVersion} != x${turboImage} ]
then
  find ${yamlBasePath}/ -name *.yaml | xargs sed -i "s/${currentVersion}/${turboVersion}/"
fi

# Production
/opt/local/bin/turboctl create base
/opt/local/bin/turboctl create services
/opt/local/bin/turboctl create probes
