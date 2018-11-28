#!/bin/bash

# turboUpgrade.sh
# Upgrade the version of Turbonomic XL.

# Get the parameters used for kubernetes, gluster, turbo setup
source /opt/local/etc/turbo.conf

# Set basepath for xl yaml
yamlBasePath="/opt/turbonomic/kubernetes/yaml"
imageBasePath="/opt/turbonomic/kubernetes/images"

# Determine if the registry is set properly
registryValue=$(grep "image: localhost" ${yamlBasePath}/base/api.yaml | awk -F: '{print $2}' | tr -d '[:space:]')
if [ ! ${registryValue} == ${registry} ]
then
  if [ "x$registry" == "xlocalhost" ]
  then 
    find ${yamlBasePath}/ -name *.yaml -exec sed -i "s/image: registry.turbonomic.com/image: localhost:5000/" {} \;
  else
    find ${yamlBasePath}/ -name *.yaml -exec sed -i "s/image: localhost:5000/image: registry.turbonomic.com/" {} \;
  fi
fi

# Load the images into docker if not using the public turbonomic registry
if [ "x${registry}" == "xlocalhost" ]
then
  for image in $(ls ${imageBasePath}/${turboVersion}/*.tgz)
  do
    sudo docker load -i ${imageBasePath}/${turboVersion}/${image}
  done
fi

# Make sure the credentials are set if pulling from the public turbonomic registry
if [ "x${registry}" == "xregistry.turbonomic.com" ]
then
  # test if the credentials exist
  kubectl get secret turbocred >/dev/null 2>&1
  credentialExists="$?"
  
  if [ ! "x${credentialExists}" == "x0" ]
  then
    kubectl create secret docker-registry turbocred --docker-server=registry.turbonomic.com --docker-username=${dockerUserName} --docker-password=${dockerPassword} --docker-email=${dockerEmail}
  fi
fi

# Update the yaml files with the proper version
if [ x$registry == xlocalhost ]
then
  currentVersion=$(grep "image:" ${yamlBasePath}/base/api.yaml | awk -F: '{print $4}')
else
  currentVersion=$(grep "image:" ${yamlBasePath}/base/api.yaml | awk -F: '{print $3}')
fi

echo $currentVersion

if [ x${currentVersion} != x${turboImage} ]
then
  find ${yamlBasePath}/ -name *.yaml | xargs sed -i "s/${currentVersion}/${turboVersion}/"
fi

# Upgrade the Turbonomic kube cluster
kubectl apply -f ${yamlBasePath}/base
kubectl apply -f ${yamlBasePath}/services
kubectl apply -f ${yamlBasePath}/probes
