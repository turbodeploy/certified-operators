FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# This is the release of Zookeeper to pull in.
ENV ZOOKEEPER_VERSION=3.5.8

# Create a zookeeper user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The /tmp/zookeeper dir is used by Zookeeper transaction log to store state.
RUN groupadd -g 1000 zookeeper && useradd -m -r -d /home/zookeeper -g 1000 -s /bin/bash -u 101 zookeeper && \
passwd -l root

COPY start_zookeeper /
COPY zookeeper-ready /

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng python2 java-11-openjdk-headless && \
    dnf clean all && \
    rm -rf /var/cache/dnf

ARG ZOOKEEPER_TAR=apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz

RUN cd /tmp && \
    curl -k -s -O http://apache.mirrors.pair.com/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/${ZOOKEEPER_TAR} && \
    tar -C /opt -xvf ${ZOOKEEPER_TAR} && \
    ln -s apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && \
    rm ${ZOOKEEPER_TAR} && \
    mkdir /tmp/zookeeper && \
    chown -R zookeeper /tmp/zookeeper && \
    mkdir /var/lib/zoo && \
    chown -R zookeeper /var/lib/zoo && \
    chmod a+x /start_zookeeper && \
    chmod a+x /zookeeper-ready

# rsyslog
ADD rsyslog.conf /etc
ADD log4j.properties /opt/zookeeper/conf

EXPOSE 2181

ENTRYPOINT ["/start_zookeeper"]

USER zookeeper
