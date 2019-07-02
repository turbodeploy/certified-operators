#!/bin/build

# Script to prepare a disk for storing mariadb database.

set -euo pipefail

MARIADB_DISK="/dev/sdc"
MARIADB_DISK_PART="/dev/sdc1"
VOLUME_GROUP="vg_turbo_db"
LVM="lv_turbo_db"
DB_LVM_PATH="/dev/mapper/$VOLUME_GROUP-$LVM"
DB_MOUNT_PATH="/var/lib/turbodb"
FSTAB="/etc/fstab"
FSLABEL="XL_MARIADB"
DISK_SIZE=500

source libs.sh

log_msg "Preparing mariadb disk"

if [! -f $MARIADB_DISK];
then
    log_msg "Disk: $MARIADB_DISK not available"
    exit 1
fi

if [ -f $MARIADB_DISK_PART];
then
    log_msg "Partition already exists: $MARIADB_DISK_PART "
    # Check if it's a valid mariadb partition that we can use
    ([[ $(blkid $MARIADB_DISK | grep 'LVM2_member') ]] && [[ $(fdisk -l  $MARIADB_DISK 2>/dev/null | grep 'Linux LVM') ]] && \
		[[ `fdisk -l  $MARIADB_DISK 2>/dev/null | grep 'Linux LVM' | awk '{print $4}' `  == "$DISK_SIZE"G]] )  \
		|| [[ log_msg "Wrong Partition Type" && exit 1 ]]
else
	log_msg "Creating lvm partition on $MARIADB_DISK"
    parted -s $MARIADB_DISK set 1 lvm on
fi

log_msg "Creating Physical Volume $MARIADB_DISK_PART"
[[ $(pvs | grep $MARIADB_DISK_PART) ]] || pvcreate $MARIADB_DISK_PART

log_msg "Creating Volume Group $MARIADB_DISK_PART"
[[ $(vgs | grep $VOLUME_GROUP) ]]  || vgcreate $VOLUME_GROUP $MARIADB_DISK_PART

log_msg "Creating Logical Volume $LVM"
[[ $(lvs | grep $LVM) ]] || lvcreate -n $LVM -l 100%FREE $VOLUME_GROUP

log_msg "Creating XFS filesystem on the LVM"
[[ $(blkid $DB_LVM_PATH | grep xfs) ]] || mkfs.xfs -L $FS_LABEL $DB_LVM_PATH

grep $DB_MOUNT_PATH /etc/fstab
if ! grep -q $DB_MOUNT_PATH $FSTAB ; then
    echo "$DB_LVM_PATH  $DB_MOUNT_PATH xfs     defaults        0 0 " >> $FSTAB
fi

log_msg "Mounting fileystem"
mount -a 

log_msg "Successfully setup disk for mariadb."
