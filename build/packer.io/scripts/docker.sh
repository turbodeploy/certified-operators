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
cat <<'EOF' >/usr/local/bin/vmtctl
#!/bin/bash
export HOST_IP=$(/sbin/ip route get 1 | awk '{print $NF;exit}')
# Start the XL component.
function start_xl_component() {
    component="$1"
    message="$2"
    echo "Starting $component component" >/tmp/load_status
    /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml up -d --no-color "${component}"
    while true
    do
        /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml logs rsyslog 2>&1 | grep "$component" | grep -q "${message}"
        if [[ $? == 0 ]]; then
            break
        fi
        sleep 5
    done
}

function set_max_load() {
    vmem=$1
    suffix=$2
    lower=$3
    upper=$4
    if [ $vmem -ge $lower ]; then
        if [ .$upper == . ] ||  [ $vmem -lt $upper ]; then
           ln -sf /etc/docker/docker-compose.yml.${suffix}k /etc/docker/docker-compose.yml
           echo "Maximum load of ${suffix}K entities is set up with ${vmem}GB" >/tmp/load_status
           sleep 10
           return 0
        fi
    fi
    return 1
}

# Master start script.
# Starts Consul, followed by the Cluster manager, followed by everything else
function start_xl() {
    # Chose the yml file based on the size
    vmem=$(cat /proc/meminfo | grep MemTotal | awk '{total=$2/1024/1024} {if(total-int(total)>0) {total=int(total)+1}}{print total}')
    if [ $vmem -lt 16 ]; then
       echo "!! Insufficient memory available with ${vmem}GB" >/tmp/load_status
       sleep 10
       return
    fi

    # 15K
    set_max_load $vmem 15 16 24
    if [ $? -eq 1 ]; then
        # 25K
        set_max_load $vmem 25 24 32
    fi
    if [ $? -eq 1 ]; then
        # 50K
        set_max_load $vmem 50 32 48
    fi
    if [ $? -eq 1 ]; then
        # 100K
        set_max_load $vmem 100 48 63
    fi
    if [ $? -eq 1 ]; then
        # 200K - no upper limit
        set_max_load $vmem 200 63
    fi

    # Check that we did attach an ISO
    if [ ! -f "/etc/docker/docker-compose.yml" ]; then
       echo "!! No component images are available" >/tmp/load_status
       sleep 10
       return
    fi

    # We assume that there will be no less than 10 images loaded on master node
    # In fact, there will be more, but at at this time we want to make sure
    # we do not just have a couple of random images loaded from docker hub on an otherwise
    # empty VM.
    NUM=$(docker images | grep latest | wc -l)
    if [ $NUM -lt 10 ]; then
       echo "!! No component images are available" >/tmp/load_status
       sleep 10
       return
    fi

    # Start all XL components. We use this order, in order to ensure that
    # API starts last, so that when we connect, all other components are already there.
    echo "Starting XL components" >/tmp/load_status
	JAVA_MSG="Component transition from STARTING to RUNNING"
	start_xl_component rsyslog "rsyslogd"
	start_xl_component consul "New leader elected"
	# docker-compose removes trailing decimals from the component name so the check will fail
	# instead, we rely on the docker-compose dependencies to start both zookeeper and kafka
#	start_xl_component zoo1 "binding to port"
#	start_xl_component kafka1 "Startup complete."
	start_xl_component clustermgr "Started ClusterMgrMain"
	start_xl_component db "port: 3306"
	start_xl_component arangodb "ready for business"
	start_xl_component auth "${JAVA_MSG}"
	start_xl_component topology-processor "$JAVA_MSG"
	start_xl_component market "$JAVA_MSG"
	start_xl_component repository "$JAVA_MSG"
	start_xl_component plan-orchestrator "$JAVA_MSG"
	start_xl_component action-orchestrator "$JAVA_MSG"
	start_xl_component group "$JAVA_MSG"
	start_xl_component history "$JAVA_MSG"
	start_xl_component mediation-hyperv "$JAVA_MSG"
	start_xl_component mediation-vcenter "$JAVA_MSG"
	start_xl_component mediation-netapp "$JAVA_MSG"
    echo "Finalizing XL components startup" >/tmp/load_status
    # Make sure the initial_setup scripts are terminated and deleted.
    if [[ -d /root/initial_setup ]]; then
      # Sleep for 10 seonds to allow the initial setup GUI to pick up the status.
      # The initial setupGUI checks every 5 seconds.
      sleep 10
      # Kill the https
      # The initial setup python script is stateless, so this won't break anything.
      curl -k "https://${HOST_IP}:443/exit" >/dev/null 2>&1
      kill -9 $(ps -e -o pid,cmd | grep [p]ython | grep httpd | awk '{print $1}') >/dev/null 2>&1
      rm -rf /root/initial_setup
    fi

    # Since we monitor for API to be up before we may proceed, we should be good
    # with starting the API component in a delayed fashion.
    # The auth component will have started by this time, so the initial setup should be okay.
	start_xl_component api "$JAVA_MSG"
}

if [[ "$1" == "init" ]]; then
  # Load the images from ISO if needed.
  mkdir /media/cdrom >/dev/null 2>&1
  echo Attempting to load the CDROM >/tmp/load_status
  mount /dev/cdrom /media/cdrom/ >/dev/null 2>&1
  if [[ $? == 0 ]]; then
      # Configure the admin user
      if [[ -d /root/initial_setup ]]; then
        echo "Setting up master admin" >/tmp/load_status
        pushd /root/initial_setup >/dev/null
        chmod +x setup
        /usr/bin/nohup ./setup >/tmp/setup_nohup.txt &
        popd >/dev/null
        while true
        do
            # Part of establishing the admin account involves removing the neteditor account.
            /usr/bin/getent passwd neteditor >/dev/null 2>&1
            if [ $? -ne 0 ]; then
                break
            fi
            echo "The initial admin account is being created"
            sleep 30
        done
        echo "Master admin has been set up" >/tmp/load_status
      fi

      echo "ISO Image has been mounted successfully" >/tmp/load_status
      # Check whether we need to do anything
      if [ -f "/media/cdrom/turbonomic_sums.txt" ]; then
         cmp -sb /media/cdrom/turbonomic_sums.txt /etc/docker/turbonomic_sums.txt
         if [[ $? == 0 ]]; then
             echo 'No changes in the ISO'
             echo Unmounting the CDROM
             umount /media/cdrom
             start_xl
         fi
      fi

      # Verify sha256 checksums
      echo "Performing integrity check" >/tmp/load_status
      pushd /media/cdrom
      sha256sum -c turbonomic_sums.txt
      CHECKSUM=$?
      popd
      if [[ $CHECKSUM == 0 ]]; then
          echo "Loading Docker images" >/tmp/load_status
          echo Docker image checksum verification has been successful
          for i in $(ls /media/cdrom/*tgz)
          do
            dimg=$(echo $i | sed 's|.*/||' | cut -d'.' -f1)
            echo "Loading $dimg component" >/tmp/load_status
            echo Loading docker image $i
            docker load -i $i
          done
          # Clean the dangling images
          docker images -qa -f 'dangling=true' | xargs docker rmi

          # We do that for multiple sizes
          if [ -f "/media/cdrom/docker-compose.yml.15k" ]; then
              mv /etc/docker/docker-compose.yml.15k /etc/docker/docker-compose.yml.15k.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/docker-compose.yml.15k /etc/docker/docker-compose.yml.15k
          fi
          if [ -f "/media/cdrom/docker-compose.yml.25k" ]; then
              mv /etc/docker/docker-compose.yml.25k /etc/docker/docker-compose.yml.25k.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/docker-compose.yml.25k /etc/docker/docker-compose.yml.25k
          fi
          if [ -f "/media/cdrom/docker-compose.yml.50k" ]; then
              mv /etc/docker/docker-compose.yml.50k /etc/docker/docker-compose.yml.50k.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/docker-compose.yml.50k /etc/docker/docker-compose.yml.50k
          fi
          if [ -f "/media/cdrom/docker-compose.yml.100k" ]; then
              mv /etc/docker/docker-compose.yml.100k /etc/docker/docker-compose.yml.100k.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/docker-compose.yml.100k /etc/docker/docker-compose.yml.100k
          fi
          if [ -f "/media/cdrom/docker-compose.yml.200k" ]; then
              mv /etc/docker/docker-compose.yml.200k /etc/docker/docker-compose.yml.200k.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/docker-compose.yml.200k /etc/docker/docker-compose.yml.200k
          fi
          if [ -f "/media/cdrom/common-services.yml" ]; then
              mv /etc/docker/common-services.yml /etc/docker/common-services.yml.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/common-services.yml /etc/docker/common-services.yml
          fi
          if [ -f "/media/cdrom/prod-services.yml" ]; then
              mv /etc/docker/prod-services.yml /etc/docker/prod-services.yml.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/prod-services.yml /etc/docker/prod-services.yml
          fi

          if [ -f "/media/cdrom/turbonomic_info.txt" ]; then
              mv /etc/docker/turbonomic_info.txt /etc/turbonomic_info.txt.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/turbonomic_info.txt /etc/docker/turbonomic_info.txt
          fi
          if [ -f "/media/cdrom/turbonomic_sums.txt" ]; then
              mv /etc/docker/turbonomic_sums.txt /etc/turbonomic_sums.txt.$(date +"%Y_%M_%d") >/dev/null 2>&1
              cp /media/cdrom/turbonomic_sums.txt /etc/docker/turbonomic_sums.txt
          fi
          echo "Docker images loaded" >/tmp/load_status
          echo Unmounting the CDROM
          umount /media/cdrom
      else
          echo "!! Component checksum verification failed" >/tmp/load_status
          exit 1
      fi
  else
      echo CDROM ISO Image is not present
  fi
  # Start the product
  start_xl
elif [[ "$1" == "info" ]]; then
  echo "Turbonomic distribution"
  echo "-----------------------"
  cat /etc/docker/turbonomic_info.txt
  echo ""
  echo "Images distributed"
  echo "------------------"
  cat /etc/docker/turbonomic_sums.txt
elif [[ "$1" == "stats" ]]; then
  if [[ $UID == 0 ]]; then
    exec docker stats $(docker ps --format '{{.Names}}')
  else
    exec docker stats $(sudo docker ps --format '{{.Names}}')
  fi
elif [[ "$1" == "shell" ]]; then
  exec docker exec -it "${@:2}" /bin/bash
elif [[ "$1" == "logs" ]]; then
  exec /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml logs -f "${@:2}"
elif [[ "$1" == "up" ]]; then
  if [[ $# -gt 1 ]]; then
    exec /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml up -d --no-color "${@:2}"
  fi
  start_xl
elif [[ "$1" == "stop" ]]; then
  if [[ $# -gt 1 ]]; then
    exec /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml stop "${@:2}"
  else
    echo "Usage: vmtctl stop <container>"
  fi
else
  echo "Usage: vmtctl <command> [arguments]"
  echo ""
  echo "  info  - Print Turbonomic XL install information"
  echo "  stats - Displays container information in a /bin/top style"
  echo "  shell <component> - Connects to a running connector"
  echo "                      and runs the /bin/bash on that container"
  echo "  logs <component>  - Displays logs for a running container, "
  echo "                      in a tail -f style"
  echo "  up <component>   - Starts an individual component"
  echo "  stop <component> - Stops an individual component"
fi
EOF
chmod +x /usr/local/bin/vmtctl
# Add a symlink, so that the sudo would see it.
ln -s /usr/local/bin/vmtctl /usr/bin/vmtctl

curl -s -L https://github.com/docker/compose/releases/download/1.11.2/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# Create docker logical volumes
pvcreate /dev/sda3
vgcreate dockdata /dev/sda3
lvcreate --wipesignatures y -n docker dockdata -l 100%VG
mkfs.xfs /dev/dockdata/docker
mkdir -p /var/lib/docker
echo "/dev/dockdata/docker /var/lib/docker    xfs     defaults        1 3" >> /etc/fstab
mount -a

# Install the specified version of Docker.
pushd /tmp
DOCKER_VERSION=1.13.1
curl -sO https://yum.dockerproject.org/repo/main/centos/7/Packages/docker-engine-${DOCKER_VERSION}-1.el7.centos.x86_64.rpm
curl -sO https://yum.dockerproject.org/repo/main/centos/7/Packages/docker-engine-selinux-${DOCKER_VERSION}-1.el7.centos.noarch.rpm
yum install -y policycoreutils-python libtool-ltdl libseccomp
rpm -i docker-engine-selinux-${DOCKER_VERSION}-1.el7.centos.noarch.rpm
rpm -i docker-engine-${DOCKER_VERSION}-1.el7.centos.x86_64.rpm
rm docker-engine-selinux-${DOCKER_VERSION}-1.el7.centos.noarch.rpm
rm docker-engine-${DOCKER_VERSION}-1.el7.centos.x86_64.rpm
popd

# Update docker daemon
sed -i 's|ExecStart.*|ExecStart=/usr/bin/docker daemon -H tcp://0.0.0.0:2375 -H unix:///var/run/docker.sock --log-driver=json-file --log-opt max-size=100m --log-opt max-file=100|' /usr/lib/systemd/system/docker.service
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
