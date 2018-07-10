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

# Master node.
mkdir -p /usr/local/bin

curl -s -L https://github.com/docker/compose/releases/download/1.11.2/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# Create docker logical volumes
pvcreate /dev/sda3
vgcreate dockdata /dev/sda3
lvcreate -y --wipesignatures y -n docker -l 100%FREE dockdata
mkfs.xfs /dev/dockdata/docker
mkdir -p /var/lib/docker
echo "/dev/dockdata/docker /var/lib/docker    xfs     defaults        1 3" >> /etc/fstab
mount -a

# Install the specified version of Docker.
pushd /tmp
DOCKER_VERSION=docker-ce-17.09.0.ce-1.el7.centos
yum install -y yum-utils \
    device-mapper-persistent-data \
    lvm2
yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo
yum install -y ${DOCKER_VERSION}
popd

# Update docker daemon
sed -i 's|ExecStart.*|ExecStart=/usr/bin/dockerd -H tcp://0.0.0.0:2375 -H unix:///var/run/docker.sock --log-driver=json-file --log-opt max-size=100m --log-opt max-file=100|' /usr/lib/systemd/system/docker.service
systemctl daemon-reload
systemctl start docker.service
systemctl enable docker.service

# Create a default network. Due to some bug in the docker-compose, it doesn't get created from the .yml file.
docker network create --driver bridge --subnet 10.10.10.0/24 --gateway 10.10.10.1 docker_default

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
