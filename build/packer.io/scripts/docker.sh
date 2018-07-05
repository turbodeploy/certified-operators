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

# if there isn't at least 16 gb of memory, log a warning and don't start any more components
MIN_VMEM=16

# http port to use for SSL connections
SSL_PORT=443

# Source and destination base for the install process
SOURCE_BASE="/media/cdrom"
DEST_BASE="/etc/docker"

# docker-compose.yml file path; also base path for docker-compose.yml.nnk files
DOCKER_COMPOSE_DEST_BASE=${DEST_BASE}/docker-compose.yml

export HOST_IP=$(/sbin/ip route get 1 | awk '{print $NF;exit}')

# list of "component" images to start (not including support containers). "api" should be last on this list
#    so that other components will all be running before the user-facing UI comes up
XL_COMPONENTS="auth topology-processor market repository plan-orchestrator action-orchestrator group \
        history reporting mediation-hyperv mediation-vcenter mediation-netapp mediation-ucs \
        mediation-vmax mediation-vmm mediation-hpe3par mediation-pure mediation-scaleio \
        mediation-hds mediation-compellent mediation-xtremio mediation-vplex mediation-rhv \
        mediation-openstack api"
# list of "support" containers to start
BASE_IMAGES="nginx rsyslog consul clustermgr db arangodb"
# how many docker images to start, totalled between base server images and XL components
N_IMAGES=$(($(echo ${XL_COMPONENTS} | wc -w) + $(echo ${BASE_IMAGES} | wc -w)))


# log a message; also write it to the one-line file read by the status monitor webpage
function log_message() {
    line_no=${BASH_LINENO[*]}
    echo $(date +"%Y-%m-%d %T") ${line_no} $@
    echo $@ > /tmp/load_status
}

# Start the XL component and verify that it has started up successfully
function start_and_verify_xl_component() {
    component="$1"
    message="$2"
    start_xl_component ${component}
    # Now wait until it's started correctly
    while true
    do
        /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml logs rsyslog 2>&1 | grep "$component" | grep -q "${message}"
        if [[ $? == 0 ]]; then
            break
        fi
        sleep 5
    done
}

# Start an XL component without waiting for the startup to complete;
#     will stop & recreate the container if image has changed
function start_xl_component() {
    component="$1"
    log_message "Starting $component component ($n_started of $N_IMAGES)"
    n_started=$(($n_started+1))
    /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml up -d --no-color "${component}"
}

# Stop an XL component
function stop_xl_component() {
    component="$1"
    log_message "Stopping ${component} component"
    /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml stop "${component}"
}

# Copy a file from source to dest, if found, making a copy of the old file
function copy_cdrom_source() {
    filename=$1
    if [ -f ${SOURCE_BASE}/${filename} ]; then
      mv ${DEST_BASE}/${filename} ${DEST_BASE}/${filename}$(date +"%Y_%M_%d") >/dev/null 2>&1
      cp ${SOURCE_BASE}/${filename} ${DEST_BASE}/${filename}
    fi
}

# If the current vmem is between the lower & upper bounds then add a symlink & return 0; else return 1
function set_max_load() {
    vmem=$1
    suffix=$2
    lower=$3
    upper=$4
    if [ ${vmem} -ge ${lower} ]; then
        if [ .${upper} == . ] ||  [ ${vmem} -lt ${upper} ]; then
           ln -sf ${DOCKER_COMPOSE_DEST_BASE}.${suffix}k ${DOCKER_COMPOSE_DEST_BASE}
           log_message "Maximum load of ${suffix}K entities is set up with ${vmem}GB"
           # pause so this message may be seen on the progress monitor webpage
           sleep 10
           return 0
        fi
    fi
    return 1
}

# Master start script.
# Starts Consul, followed by the Cluster manager, followed by everything else
function start_xl() {

    # Verify that we did identify a docker-compose file to link to
    if [ ! -f "${DOCKER_COMPOSE_DEST_BASE}" ]; then
       log_message "!! No component configuration available"
       return
    fi

    # Sanity-check that the correct number of docker images are available
    NUM=$(docker images | grep latest | wc -l)
    if [ ${NUM} -lt ${N_IMAGES} ]; then
       log_message "!! No component images are available"
       return
    fi

    # Chose the yml file based on the size
    vmem=$(cat /proc/meminfo | grep MemTotal | awk '{total=$2/1024/1024} {if(total-int(total)>0) {total=int(total)+1}}{print total}')

    # if there isn't sufficient memory, log a warning and don't start any more components
    if [ ${vmem} -lt ${MIN_VMEM} ]; then
       log_message "!! Insufficient memory available with ${vmem}GB"
       return
    fi

    # 15K config applies to memory sizes under 24 mb
    set_max_load ${vmem} 15 1 24
    if [ $? -eq 1 ]; then
        # 25K
        set_max_load ${vmem} 25 24 32
    fi
    if [ $? -eq 1 ]; then
        # 50K
        set_max_load ${vmem} 50 32 48
    fi
    if [ $? -eq 1 ]; then
        # 100K
        set_max_load ${vmem} 100 48 63
    fi
    if [ $? -eq 1 ]; then
        # 200K - no upper limit
        set_max_load ${vmem} 200 63
    fi

    # Progress feedback for starting components; how many started so far
    n_started=1

    # start nginx; if was already running (and container hasn't changed) will not stop previous image
    log_message "Starting web server"
    start_xl_component nginx

    # Start the remaining XL components. It is assumed that "api" is last on the list so that
    # API component starts last. Thus when we connect, all other components are already running.
    log_message "Starting XL components"
    JAVA_MSG="Component transition from STARTING to RUNNING"
    start_and_verify_xl_component rsyslog "rsyslogd"
    start_and_verify_xl_component consul "New leader elected"
    # docker-compose removes trailing decimals from the component name so the check will fail
    # instead, we rely on the docker-compose dependencies to start both zookeeper and kafka
#	start_xl_component zoo1 "binding to port"
#	start_xl_component kafka1 "Startup complete."
    start_and_verify_xl_component clustermgr "Server.*: Started"
    start_and_verify_xl_component db "port: 3306"
    start_and_verify_xl_component arangodb "ready for business"

    # start all components; the API component should be last so other components are all "ready"
    #    when the UI comes up
    for component in ${XL_COMPONENTS}; do
        start_and_verify_xl_component ${component} "${JAVA_MSG}"
    done

    log_message "System was successfully started."

    # wait a bit, then set one last status
    sleep 30
    log_message "Turbonomic is running."
}

if [[ "$1" == "init" ]]; then
  # Configure the admin user, if this wasn't already done
  if [[ -d /root/initial_setup ]]; then
    log_message "Setting up master admin"
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
    log_message "Master admin has been set up"
    # Make sure the initial_setup scripts are terminated and deleted.
    # Sleep for 10 seonds to allow the initial setup GUI to pick up the status.
    # The initial setupGUI checks every 5 seconds.
    sleep 10
    # Kill the https
    # The initial setup python script is stateless, so this won't break anything.
    curl -k "https://${HOST_IP}:${SSL_PORT}/exit" >/dev/null 2>&1
    kill -9 $(ps -e -o pid,cmd | grep [p]ython | grep httpd | awk '{print $1}') >/dev/null 2>&1
    rm -rf /root/initial_setup
  fi

  log_message "Stopping the previously running system, if any"
  /usr/local/bin/docker-compose -f /etc/docker/docker-compose.yml stop ${XL_COMPONENTS}

  # start nginx; note that this will be the "old" (i.e. pre-load) nginx
  log_message "Starting nginx"
  n_started=1
  start_xl_component nginx

  # Load docker images from ISO if needed.
  mkdir /media/cdrom >/dev/null 2>&1
  log_message Attempting to load the CDROM
  umount /media/cdrom >/dev/null 2>&1
  mount /dev/cdrom /media/cdrom/ >/dev/null 2>&1
  if [[ $? == 0 ]]; then
      log_message "ISO Image has been mounted successfully"
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
      log_message "Performing integrity check"
      pushd /media/cdrom
      sha256sum -c turbonomic_sums.txt
      CHECKSUM=$?
      popd
      if [[ ${CHECKSUM} == 0 ]]; then
          log_message "Loading Docker images"
          echo Docker image checksum verification has been successful
          images=$(ls /media/cdrom/*tgz)
          n=1
          m=$(echo ${images} | wc -w)
          for i in ${images}
          do
            dimg=$(echo ${i} | sed 's|.*/||' | cut -d'.' -f1)
            log_message "Loading $dimg component ($n of $m)"
            echo Loading docker image ${i}
            docker load -i ${i}
            n=$((n+1))
          done
          # Clean the dangling images
          log_message "Cleaning old docker images"
          docker images -qa -f 'dangling=true' | xargs docker rmi

          # Copy the docker-compose.yml files for the available memory sizes
          copy_cdrom_source docker-compose.yml.15k
          copy_cdrom_source docker-compose.yml.25k
          copy_cdrom_source docker-compose.yml.50k
          copy_cdrom_source docker-compose.yml.100k
          copy_cdrom_source docker-compose.yml.200k

          copy_cdrom_source common-services.yml
          copy_cdrom_source prod-services.yml
          copy_cdrom_source turbonomic_info.txt
          copy_cdrom_source turbonomic_sums.txt

          # Copy the 'turboctl' command-line utility, make it executable, and add a symlink for it
          copy_cdrom_source turboctl.py
          chmod +x ${DEST_BASE}/turboctl.py
          if [ ! -f /usr/bin/turboctl ]; then
             ln -s ${DEST_BASE}/turboctl.py /usr/bin/turboctl
          fi

          log_message "Docker images loaded"
          echo Unmounting the CDROM
          umount /media/cdrom
      else
          log_message "!! Component checksum verification failed"
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
  cat ${DEST_BASE}/turbonomic_info.txt
  echo ""
  echo "Images distributed"
  echo "------------------"
  cat ${DEST_BASE}/turbonomic_sums.txt
elif [[ "$1" == "stats" ]]; then
  if [[ $UID == 0 ]]; then
    exec docker stats $(docker ps --format '{{.Names}}')
  else
    exec docker stats $(sudo docker ps --format '{{.Names}}')
  fi
elif [[ "$1" == "shell" ]]; then
  exec docker exec -it "${@:2}" /bin/bash
elif [[ "$1" == "logs" ]]; then
  exec /usr/local/bin/docker-compose -f ${DEST_BASE}/docker-compose.yml logs -f "${@:2}"
elif [[ "$1" == "up" ]]; then
  # discard the command "up"
  shift
  if [[ $# -gt 0 ]]; then
    exec /usr/local/bin/docker-compose -f ${DEST_BASE}/docker-compose.yml up -d --no-color "$@"
  else
    start_xl
  fi
elif [[ "$1" == "stop" ]]; then
  if [[ $# -gt 1 ]]; then
    exec /usr/local/bin/docker-compose -f ${DEST_BASE}/docker-compose.yml stop "${@:2}"
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
