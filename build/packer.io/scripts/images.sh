#!/usr/bin/env bash
# preload the turbonomic images into the host.

# mount the source iso
echo "Mount ISO for install"
mkdir /mnt/xl
mount -t iso9660 /dev/sr1 /mnt/xl

# Load the images from /mnt/xl/
for i in $(ls /mnt/xl/*tgz)
do
    dimg=$(echo $i | sed 's|.*/||' | cut -d'.' -f1)
    echo "Loading image $dimg into local registry"
    docker load -i $i
done

# copy the docker config files
echo "Copying docker configuration files"
cp /mnt/xl/docker-compose.yml.* /etc/docker/
cp /mnt/xl/common-services.yml /etc/docker/common-services.yml
cp /mnt/xl/prod-services.yml /etc/docker/prod-services.yml
cp /mnt/xl/turbonomic_info.txt /etc/docker/turbonomic_info.txt
cp /mnt/xl/turbonomic_sums.txt /etc/docker/turbonomic_sums.txt

# link to the 16gb compose file so we have a minimum config ready for immediate use
ln -sf /etc/docker/docker-compose.yml.15k /etc/docker/docker-compose.yml

echo "Finished loading images and docker config files"
