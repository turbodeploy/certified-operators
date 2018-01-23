FROM ubuntu:17.10
MAINTAINER Michael Borodiansky <michael.borodiansky@turbonomic.com>

# This is the release of Consul to pull in.
ENV CONSUL_VERSION=0.8.3
# This is the release of the HashiCorp-built version of dumb-init.
ENV DOCKER_DUMBINIT=1.2.0

RUN apt-get update -q -y && apt-get upgrade -q -y && \
    apt-get install -q -y openssl unzip iproute2 wget bash rsyslog && \
    cd /tmp && \
    wget -q -O consul.zip https://releases.hashicorp.com/consul/${CONSUL_VERSION}/consul_${CONSUL_VERSION}_linux_amd64.zip && \
    unzip -d /bin consul.zip && \
    rm consul.zip && \
    set -x \
    cd /tmp && \
    wget -q -O /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v${DOCKER_DUMBINIT}/dumb-init_${DOCKER_DUMBINIT}_amd64 && \
    chmod +x /bin/dumb-init && \
    apt-get purge -y --auto-remove wget unzip && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

# Create a consul user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The /consul/data dir is used by Consul to store state. The agent will be started
# with /consul/config/local as the configuration directory so you can add additional
# config files in that location. There are client and server-specific locations so
# we can supply some default configs via this base image as well.
RUN addgroup consul && \
    adduser --system --disabled-password -home /home/consul --ingroup consul --disabled-login --shell /bin/bash consul && \
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
