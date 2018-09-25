#!/bin/bash

# Script: t8s.sh
# Author: Billy O'Connell
# Purpose: Setup a kubernetes environment with T8s xl components
# Tools:  Kubespray, Heketi, GlusterFs

# Get the parameters used for kubernetes, gluster, turbo setup
source /opt/local/etc/turbo.conf

# Functions
usage()
{
        echo "Use: `basename $0`"
        exit -1
}

pause()
{
    key=""
    echo
    echo -n "Configuration confirmation, press any key to continue with the install"
    stty -icanon
    key=`dd count=1 2>/dev/null`
    stty icanon
}

# Variables:
kubesprayPath="/opt/kubespray"
inventoryPath="${kubesprayPath}/inventory/turbocluster"
glusterStorage="/opt/gluster-kubernetes"
glusterStorageJson="${glusterStorage}/deploy/topology.json"
declare -a node=(${node})

# Build the node array to pass into kubespray
for i in "${node[@]}"
do
  export node${#node[@]}=${i}
done
# get length of an array
tLen=${#node[@]}

# Check that the proper amount of ip addresses were used.
if (( ${tLen} > ${nodeAnswer} ))
then
  echo "===================================================================================="
  echo "The number of ip addresses given is greater than the total amount of nodes provided."
  echo "===================================================================================="
  exit
fi
if (( ${tLen} < ${nodeAnswer} ))
then
  echo "====================================================================="
  echo "The number of ip addresses given does not meet the node requirements."
  echo "====================================================================="
  exit
fi


# List the master nodes:
echo
echo
echo "Master Node(s)"
echo "++++++++++++"
if (( ${tLen} > 1 ))
then
  for ((i=0,j=1; i<2; i++,j++));
  do
    echo node${j} ${node[i]}
  done
  echo
  echo
else
  echo node1 ${node[0]}
  echo
  echo
fi

# List the kubelet nodes
echo "Worker Nodes"
echo "++++++++++++"
for ((i=0,j=1; i<${#node[*]}; i++,j++));
do
    echo node${j} ${node[i]}
done

# Run kubespray
pushd ${kubesprayPath}

# Clear old host.ini file
rm -rf ${kubesprayPath}/inventory/turbocluster
cp -rfp ${kubesprayPath}/inventory/sample ${inventoryPath}
CONFIG_FILE=inventory/turbocluster/hosts.ini python3.6 contrib/inventory_builder/inventory.py ${node[@]}

# Adjust for relaxing the number of dns server allowed
cp ${kubesprayPath}/roles/docker/defaults/main.yml ${kubesprayPath}/roles/docker/defaults/main.yml.orig
dns_strict="docker_dns_servers_strict: true"
dns_not_strick="docker_dns_servers_strict: false"
dns_not_strick_group="#docker_dns_servers_strict: false"
sed -i "s/${dns_strict}/${dns_not_strick}/g" ${kubesprayPath}/roles/docker/defaults/main.yml
sed -i "s/${dns_strict}/${dns_not_strick_group}/g" ${inventoryPath}/group_vars/all.yml

# Run ansible kubespray install
ansible-playbook -i inventory/turbocluster/hosts.ini cluster.yml
popd

# Setup storage
# These need to be done on each node
for ((i=0,j=1; i<(${#node[*]}-1); i++,j++));
do
   ssh root@${node[$i]} modprobe dm_thin_pool
   ssh root@${node[$i]} modprobe dm_snapshot
   ssh root@${node[$i]} setsebool -P virt_sandbox_use_fusefs on
done

# For new installs, make sure disk is clean
vgroup=$(vgdisplay | grep "VG Name" | awk '{print $3}')
for i in ${vgroup[@]}
do
  if [ $i != turbo ]
  then
    vgremove -f ${i}
  fi
done
wipefs -a /dev/sdb

# Setup GlusterFS Native Storage Service for Kubernetes
if (( ${tLen} > 1 ))
then
  for ((i=0,j=1; i<(${#node[*]}-1); i++,j++));
  do
    cat << EOF >> /tmp/topology.json
        {
          "node": {
            "hostnames": {
              "manage": [
                "node${j}"
              ],
              "storage": [
                "${node[i]}"
              ]
            },
            "zone": 1
          },
          "devices": [
            "${device}"
          ]
        },
EOF
  done
fi
# For the last node, leave out the common for valid json file
lastNodeElement="${node[-1]}"
cat << EOF >> /tmp/topology.json
        {
          "node": {
            "hostnames": {
              "manage": [
                "node${tLen}"
              ],
              "storage": [
                "${lastNodeElement}"
              ]
            },
            "zone": 1
          },
          "devices": [
            "${device}"
          ]
        }
EOF

cp "${glusterStorageJson}.template" "${glusterStorageJson}"
sed -i '/nodes/r /tmp/topology.json' "${glusterStorageJson}"
rm -rf /tmp/topology.json

# Run the heketi/gluster setup
pushd ${glusterStorage}/deploy
if (( ${tLen} >= 1 ))
then
  /opt/gluster-kubernetes/deploy/gk-deploy --single-node -gyv
else
  /opt/gluster-kubernetes/deploy/gk-deploy -gyv
fi
popd

# Installation Complete
echo
echo
echo "######################################################################"
echo "                   Kubernetes  Installation Complete                         "
echo "######################################################################"
# Nodes
echo
echo STATUS
echo
echo "****************************** Nodes ******************************"
kubectl get nodes
echo "*******************************************************************"
echo
echo
echo "************************** Storage Class **************************"
kubectl get sc
echo "*******************************************************************"
echo
echo
echo "*************************** Service *******************************"
kubectl get svc
echo "*******************************************************************"
echo
echo
echo "************************* Deployments *****************************"
kubectl get deployments
echo "*******************************************************************"

if [ "x${node[0]}" != "x10.0.2.15" ]
then
  # Install pre-turbonomic environmental requirementes
  echo
  echo
  echo
  echo "######################################################################"
  echo "                 Prepare Turbonomic Appliance                         "
  echo "######################################################################"
  /opt/local/bin/turboEnv.sh
  echo
  echo
  echo

  # Install local registry if needed
  echo "######################################################################"
  echo "                 Setup Registry ${registry}                           "
  echo "######################################################################"
  /opt/local/bin/turboRegistry.sh
  echo
  echo
  echo

  # Install turbo components
  echo "######################################################################"
  echo "                 Start Turbonomic Deployment                          "
  echo "######################################################################"
  /opt/local/bin/turboServices.sh
fi
