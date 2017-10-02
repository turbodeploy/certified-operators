#!/bin/sh
# Close the ICMP timestamp hole
# The rules must be saved before docker is installed, since
# docker will create its own iptables rules, and we don't want
# to save those.
yum install -y iptables-services
yum install -y iptables-utils
iptables -A INPUT -p icmp --icmp-type timestamp-request -j DROP
iptables -A OUTPUT -p icmp --icmp-type timestamp-reply -j DROP
iptables-save > /etc/sysconfig/iptables
systemctl start iptables.service
systemctl enable iptables.service

cat <<EOF >/etc/yum.repos.d/docker.repo
[dockerrepo]
name=Docker Repository
baseurl=https://yum.dockerproject.org/repo/main/centos/7/
enabled=1
gpgcheck=1
gpgkey=https://yum.dockerproject.org/gpg
EOF
yum -y install docker-engine

mkdir -p /usr/local/bin
cat <<EOF >/usr/local/bin/vmtctl
#!/bin/sh
export HOST_IP=\$(ip route get 1 | awk '{print \$NF;exit}')

function log() {
	logger "\$@"
	echo "\$@"
}

if [[ "\$1" == "init" ]]; then
  # Configure the admin user
  if [[ -d /root/initial_setup ]]; then
    pushd /root/initial_setup >/dev/null
    chmod +x setup
    ./setup
    RC=\$?
    popd >/dev/null
    if [[ \$RC == 0 ]]; then
        rm -rf /root/initial_setup
        # Remove the special user.
        userdel neteditor
        rm -rf /home/neteditor
        # Remove the netuser login binary and copies of the /etc/sudoers
        # which were put into /opt
        rm -f /opt/netedit
        cp /opt/sudoers.backup /etc/sudoers
        rm -f /opt/sudoers
        rm -f /opt/sudoers.backup
    fi
  fi
  mkdir /media/cdrom >/dev/null 2>&1
  log Attempting to load the CDROM
  mount /dev/cdrom /media/cdrom/ >/dev/null 2>&1
  if [[ \$? == 0 ]]; then
      log ISO Image has been mounted successfully
      # Check whether we need to do anything
      if [ -f "/media/cdrom/turbonomic_sums.txt" ]; then
         cmp -sb /media/cdrom/turbonomic_sums.txt /etc/docker/turbonomic_sums.txt
         if [[ \$? == 0 ]]; then
             log 'No changes in the ISO'
             log Unmounting the CDROM
             umount /media/cdrom
             exit 0
         fi
      fi

      # Verify sha256 checksums
      pushd /media/cdrom
      sha256sum -c turbonomic_sums.txt
      CHECKSUM=\$?
      popd
      if [[ \$CHECKSUM == 0 ]]; then
          log Docker image checksum verification has been successful
          for i in \$(ls /media/cdrom/*tgz); do logger Loading docker image \$i; docker load -i \$i; done
          if [ -f "/media/cdrom/docker-compose.yml" ]; then
              mv /etc/docker/docker-compose.yml /etc/docker/docker-compose.yml.\$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/docker-compose.yml /etc/docker/docker-compose.yml
          fi
          if [ -f "/media/cdrom/turbonomic_info.txt" ]; then
              mv /etc/docker/turbonomic_info.txt /etc/turbonomic_info.txt.\$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/turbonomic_info.txt /etc/docker/turbonomic_info.txt
          fi
          if [ -f "/media/cdrom/turbonomic_sums.txt" ]; then
              mv /etc/docker/turbonomic_sums.txt /etc/turbonomic_sums.txt.\$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/turbonomic_sums.txt /etc/docker/turbonomic_sums.txt
          fi
          log Unmounting the CDROM
          umount /media/cdrom
      else
          log Docker image checksum verification has failed
      fi
  else
      log CDROM ISO Image is not present
  fi
  exec /usr/local/bin/impl_docker_compose -f /etc/docker/docker-compose.yml "up" -d --no-color
elif [[ "\$1" == "info" ]]; then
  echo "Turbonomic distribution"
  echo "-----------------------"
  cat /etc/docker/turbonomic_info.txt
  echo ""
  echo "Images distributed"
  echo "------------------"
  cat /etc/docker/turbonomic_sums.txt
elif [[ "\$1" == "stats" ]]; then
  exec docker stats \$(docker ps —format ‘{{.Names}}’)
elif [[ "\$1" == "shell" ]]; then
  exec docker exec -it "\${@:2}" /bin/bash
elif [[ "\$1" == "up" ]]; then
  exec /usr/local/bin/impl_docker_compose -f /etc/docker/docker-compose.yml up -d --no-color "\${@:2}"
else
  exec /usr/local/bin/impl_docker_compose -f /etc/docker/docker-compose.yml "\$@"
fi
EOF
chmod +x /usr/local/bin/vmtctl
# Add a symlink, so that the sudo would see it.
ln -s /usr/local/bin/vmtctl /usr/bin/vmtctl

curl -s -L https://github.com/docker/compose/releases/download/1.9.0/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/impl_docker_compose
chmod +x /usr/local/bin/impl_docker_compose

# Create logical volumes
pvcreate /dev/sda3
vgcreate docker /dev/sda3
lvcreate --wipesignatures y -n thinpool docker -l 95%VG
lvcreate --wipesignatures y -n thinpoolmeta docker -l 1%VG
lvconvert -y --zero n -c 512K --thinpool docker/thinpool --poolmetadata docker/thinpoolmeta

# Container data logical volumes.
# The /dev/sda4 is being skipped and unavailable.
pvcreate /dev/sda5
vgcreate dockdata /dev/sda5
lvcreate --wipesignatures y -n volumes dockdata -l 100%VG
mkfs.xfs /dev/dockdata/volumes
mkdir -p /var/lib/docker/volumes
echo "/dev/dockdata/volumes /var/lib/docker/volumes    xfs     defaults        1 3" >> /etc/fstab
mount -a

# Update docker daemon
sed -i 's|ExecStart.*|ExecStart=/usr/bin/docker daemon --storage-opt dm.thinpooldev=/dev/mapper/docker-thinpool --storage-opt dm.use_deferred_removal=true --storage-opt dm.use_deferred_deletion=true -H tcp://0.0.0.0:2375 -H unix:///var/run/docker.sock|' /usr/lib/systemd/system/docker.service
systemctl daemon-reload
systemctl start docker.service
systemctl enable docker.service

# Create a default network. Due to some bug in the docker-compose, it doesn't get created from the .yml file.
#docker network create --driver bridge --subnet 10.10.10.0/24 --gateway 10.10.10.1 docker_default

# The turbonomic service
cat <<EOF >/usr/lib/systemd/system/turbonomic.service
[Unit]
Description=Turbonomic
Documentation=https://www.turbonomic.com
After=docker.service
Requires=docker.service

[Service]
Type=notify
ExecStart=/usr/local/bin/vmtctl init
MountFlags=slave
LimitNOFILE=1048576
LimitNPROC=1048576
LimitCORE=infinity
TimeoutStartSec=0
# set delegate yes so that systemd does not reset the cgroups of docker containers
Delegate=yes

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable turbonomic.service

echo "Docker setup complete."
