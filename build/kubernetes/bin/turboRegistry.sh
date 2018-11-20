#!/bin/bash

# turboRegistry.sh
# Install a local registry

# Get the parameters used for kubernetes, gluster, turbo setup
source /opt/local/etc/turbo.conf

# Set basepath for xl yaml
yamlBasePath="/opt/turbonomic/kubernetes/yaml"

if [ x$registry == xlocalhost ]
then
  # Deploy local kubernetes registry
  kubectl create -f ${yamlBasePath}/registry/

  # Test for local registry running
  echo "Wait for local registry to be ready"
  echo "-----------------------------------"
  while [ ! ${completedRegistry} ]
  do
    completedRegistry=$(kubectl get pod | egrep -v "proxy|NAME" | awk '{print $3}' | grep Running)
    if [ ! -z ${completedRegistry} ]
    then
      echo "Completed, Local Registry is Ready"
    else
      sleep 2
    fi
  done

  find ${yamlBasePath}/ -name *.yaml -exec sed -i "s/image: registry.turbonomic.com/image: localhost:5000/" {} \;
  echo
  echo
  echo "Loading docker images into the local registry"
  echo "---------------------------------------------"
  /opt/local/bin/registryctl bulk all ${turboVersion}
else
  kubectl create secret docker-registry turbocred --docker-server=registry.turbonomic.com --docker-username=${dockerUserName} --docker-password=${dockerPassword} --docker-email=${dockerEmail}
  find ${yamlBasePath}/ -name *.yaml -exec sed -i "s/image: localhost:5000/image: registry.turbonomic.com/" {} \;
fi
