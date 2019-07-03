#!/bin/bash

# Script to externalize MariaDB : migrate from k8s and gluster volume to run outside
# k8s environment on the VM with local storage.

set -eou pipefail
CODE_DIR="/opt/local/bin"
MY_CNF="/etc/my.cnf.d/server.cnf"
MYSQL_DATA_DIR="/var/lib/mysql/mysql"
MYSQL_VAR_RUN="/var/run/mysqld"
TMP_GLUSTER_MOUNT_LOC="/mnt/mariadb-gluster"

source $CODE_DIR/libs.sh

function umount_gluster {
    if mountpoint -q $TMP_GLUSTER_MOUNT_LOC;then
        log_msg  "Unmounting glusterfs"
        sudo umount $TMP_GLUSTER_MOUNT_LOC
    fi
    [[ -d $TMP_GLUSTER_MOUNT_LOC ]] && sudo rmdir $TMP_GLUSTER_MOUNT_LOC
}

trap umount_gluster EXIT

echo
log_msg "------Migrating MariaDB data from glusterfs to local disk.----"
# Check if mariadb is installed
if ! yum list -q installed Mariadb-server 2>/dev/null
then
    log_msg "Mariadb server package is not installed. Aborting."
    exit 1
fi


if [[ ! -z "$(sudo ls -A $MYSQL_DATA_DIR)" ]]; then
    log_msg "Error: Mariadb data directory already exists and is not empty. Aborting."
    exit 1
fi

# Get the gluster volume where the existing db data is stored.
gluster_db_volume=$(kubectl get pvc | grep db-data | awk '{print $3}')
gluster_volume_path=$(mount | grep $gluster_db_volume | awk  '{print $1}')

# Shutdown all XL pods including the DB pod so that no DB transactions are on-going.
for pod in $(kubectl get deployments -n turbonomic | awk '{print $1}' | egrep -v 'NAME|t8c-operator' )
do
    kubectl scale deployment $pod --replicas=0
done
log_msg "Waiting for the pods to terminate..."
sleep 60

#Verify that the db deployment is shutdown
db_ready=$(kubectl get deployments -n turbonomic | awk '$1=="db" && $3==0' | wc -l)
if [[ $db_ready -ne 1 ]]; then
    log_msg "DB deployment is still running even after shutdown. Aborting."
    exit 1
fi

sudo mkdir -p $TMP_GLUSTER_MOUNT_LOC
if ! mountpoint -q $TMP_GLUSTER_MOUNT_LOC; then
    log_msg "Mounting mariadb gluster volume"
    # Mount read-only for safety
    sudo mount -t glusterfs $gluster_volume_path $TMP_GLUSTER_MOUNT_LOC  -o ro
fi

# Create data dir
sudo mkdir -p -m 700 $MYSQL_DATA_DIR
sudo chown -R mysql:mysql $MYSQL_DATA_DIR

# Create directory for lock file and sockets.
sudo mkdir -p -m 700 $MYSQL_VAR_RUN
sudo chown mysql:mysql $MYSQL_VAR_RUN

# Move my.cnf to server.conf
sudo cp $TMP_GLUSTER_MOUNT_LOC/my.cnf $MY_CNF
sudo chown -R mysql:mysql $MY_CNF
if ! sudo grep $MYSQL_DATA_DIR $MY_CNF ; then
    sudo sed -i  's/\/var\/lib\/mysql/\/var\/lib\/mysql\/mysql/g' $MY_CNF
fi

sudo chmod 700 $MY_CNF
pushd .
cd $TMP_GLUSTER_MOUNT_LOC
log_msg "Copying DB data from gluster volume to local disk on the VM."
sudo cp -a . $MYSQL_DATA_DIR/
popd
sudo chown -R mysql:mysql $MYSQL_DATA_DIR

mariadb_tmp_dir=$(sudo grep tmpdir $MY_CNF  | awk '{print $NF}')
sudo mkdir -p -m 700 $mariadb_tmp_dir
sudo chown -R mysql:mysql $mariadb_tmp_dir

configure_buffer_pool $MY_CNF
[[ -L /share ]] ||  sudo ln -s  /usr/share /share

log_msg "Starting MariaDB server."
sudo systemctl restart  mariadb

log_msg "Successfully migrated mariadb."

