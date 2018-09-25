#!/bin/bash

# turboUpdate.sh
# Set up the kubernetes namespace, network policies and registry to use.

# Get the parameters used for kubernetes, gluster, turbo setup
source /opt/local/etc/turbo.conf

# Set basepath for xl yaml
yamlBasePath="/opt/xl/kubernetes/yaml"
imageBasePath="/opt/xl/kubernetes/images"

# Get current image (this will have to be adjusted if we move to individual versioning
mkdir -p ${imageBasePath}/${turboVersion}
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

kubectl apply -f ${yamlBasePath}/base/
kubectl apply -f ${yamlBasePath}/services
kubectl apply -f ${yamlBasePath}/probes

