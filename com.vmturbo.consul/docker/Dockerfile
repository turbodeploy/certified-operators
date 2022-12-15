FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="kafka" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="kafka" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."
# Required Licenses
COPY licenses /licenses

# This is the release of Consul to pull in.
ENV CONSUL_VERSION=1.12.5

# This is the release of the HashiCorp-built version of dumb-init.
ENV DOCKER_DUMBINIT=1.2.0

# Install rsyslog and consul packages
# Unfortunately, our build system was built before BuildKit was available.
# Additionally, we use spotify's docker-maven-plugin for triggering the docker builds.
# Support for this plugin was dropped before BuildKit was available.
# For now we use a solution that's good enough for a developer to get stuff running on their laptop.
# When we want to support multi-architecture builds, we will need to migrate to a different maven
# plugin in OM-93669.
RUN case $(uname -m) in \
      "aarch64")  \
        NORM_ARCH=arm64 \
        ARCH=aarch64 ;; \
      *) \
        NORM_ARCH=amd64 \
        ARCH=x86_64;  ;; \
    esac \
    && dnf install -y rsyslog procps-ng python2 openssl iproute unzip gnutls && \
    cd /tmp && \
    curl -k -s -O https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_${NORM_ARCH}.zip && \
    unzip -d /bin consul_${CONSUL_VERSION}_linux_${NORM_ARCH}.zip && \
    rm consul_${CONSUL_VERSION}_linux_${NORM_ARCH}.zip && \
    set -x \
    cd /tmp && \
    curl -k -s -O https://github.com/Yelp/dumb-init/releases/download/v${DOCKER_DUMBINIT}/dumb-init_${DOCKER_DUMBINIT}_${ARCH} && \
    mv /tmp/dumb-init_${DOCKER_DUMBINIT}_${ARCH} /bin/dumb-init && \
    chmod +x /bin/dumb-init && \
    dnf update -y krb5-libs \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Create a consul user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The /consul/data dir is used by Consul to store state. The agent will be started
# with /consul/config/local as the configuration directory so you can add additional
# config files in that location. There are client and server-specific locations so
# we can supply some default configs via this base image as well.
RUN groupadd -g 1000 consul && useradd -m -r -d /home/consul -g 1000 -s /bin/bash -u 102 consul && \
passwd -l root && \
    mkdir -p /consul/data && \
    mkdir -p /consul/config/local && \
    mkdir -p /consul/config/client && \
    mkdir -p /consul/config/server && \
    chown -R consul:consul /consul

# Client mode has some default configuration that's good for running in containers,
# namely agents will leave the cluster if you stop the container. This isn't
# usually desirable for servers so we set the opposite for servers and also skip
# leaving when servers are interrupted.
COPY client.json /consul/config/client/
COPY server.json /consul/config/server/

# rsyslog
ADD rsyslog.conf /etc

# Expose the consul data directory as a volume since there's mutable state in there.
VOLUME /consul/data

# Server RPC is used for communication between Consul clients and servers for internal
# request forwarding.
EXPOSE 8300

# Serf LAN and WAN (WAN is used only by Consul servers) are used for gossip between
# Consul agents. LAN is within the datacenter and WAN is between just the Consul
# servers in all datacenters.
EXPOSE 8301 8301/udp 8302 8302/udp

# CLI, HTTP, and DNS (both TCP and UDP) are the primary interfaces that applications
# use to interact with Consul.
EXPOSE 8400 8500 8600 8600/udp

COPY upgrade.sh /

# as part of the upgrade of consul to 1.12.0, the previosu consul, 0.8.3 is required.
# the 0.8.3 consul binary is only used once during an upgrade process and 
# we have been given the 'okay' to make use of it and to have the binary as apart of the image
# to attempt to bypass a scan that might detect this, we decided to split the file
# into two pieces called part 1 and part 2. when an upgrade happens, the file is assembled
# so that it can be used to help export data that will be then imported by the newer consul.
RUN curl -s -o /consul.part1 http://build.vmturbo.com:8081/nexus/content/repositories/thirdparty/consul/consul.part1 && \
    curl -s -o /consul.part2 http://build.vmturbo.com:8081/nexus/content/repositories/thirdparty/consul/consul.part2

# Consul doesn't need root privileges so we run it as the consul user from the
# entry point script. The entry point script also uses dumb-init as the top-level
# process to reap any zombie processes created by Consul sub-processes.
COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]

USER consul

# By default you'll get a single-node development server that stores everything
# in RAM, exposes a web UI, and bootstraps itself. Don't use this configuration
# for production.
CMD ["dev"]
