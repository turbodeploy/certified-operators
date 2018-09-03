#!/bin/bash 

# turboEnv.sh
# Set up the kubernetes namespace, network policies and registry to use.

# Get the parameters used for kubernetes, gluster, turbo setup
source /opt/local/etc/turbo.conf

# Set basepath for xl yaml
yamlBasePath="/opt/xl/kubernetes/yaml"

declare -a node=(${node})

# Build the node array to pass into kubespray
for i in "${node[@]}"
do
  export node${#node[@]}=${i}
done
# get length of an array
tLen=${#node[@]}

# Set up the storage class for gluster
export HEKETI_CLI_SERVER=$(kubectl get svc/heketi --template 'http://{{.spec.clusterIP}}:{{(index .spec.ports 0).port}}')
cp ${yamlBasePath}/storage-class/gluster-heketi-sc.yaml.template ${yamlBasePath}/storage-class/gluster-heketi-sc.yaml
sed -i "s#HEKETI_CLI_SERVER#${HEKETI_CLI_SERVER}#g" ${yamlBasePath}/storage-class/gluster-heketi-sc.yaml
if [ ${tLen} = 1 ]
then
  sed -i "s#GLUSTER_REPLICA#none#g" ${yamlBasePath}/storage-class/gluster-heketi-sc.yaml
else
  sed -i "s#GLUSTER_REPLICA#replicate:${tLen}#g" ${yamlBasePath}/storage-class/gluster-heketi-sc.yaml
fi
kubectl create -f ${yamlBasePath}/storage-class/gluster-heketi-sc.yaml
echo "heketi api server = ${HEKETI_CLI_SERVER}"

# Set gluster as the default storage class
kubectl patch storageclass gluster-heketi -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

# Set namespace
kubectl create -f ${yamlBasePath}/namespace/turbo.yaml
kubectl config set-context turbo --namespace=${namespace}
kubectl config use-context turbo

# Set gluster as the default storage class
kubectl patch storageclass gluster-heketi -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

# Set network poilcy
kubectl create -f ${yamlBasePath}/network-policy

# Get current image (this will have to be adjusted if we move to individual versioning
currentVersion=$(grep "image:" ${yamlBasePath}/base/api.yaml | awk -F: '{print $4}')

if [ x${currentVersion} != x${turboImage} ]
then
  find ${yamlBasePath}/ -name *.yaml | xargs sed -i "s/${currentVersion}/${turboVersion}/"
fi
