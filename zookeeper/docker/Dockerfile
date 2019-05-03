FROM registry.access.redhat.com/rhel7
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# This is the release of Zookeeper to pull in.
ENV ZOOKEEPER_VERSION=3.4.10

# Create a zookeeper user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The /tmp/zookeeper dir is used by Zookeeper transaction log to store state.
RUN groupadd -g 1000 zookeeper && useradd -m -r -d /home/zookeeper -g 1000 -s /bin/bash -u 101 zookeeper && \
passwd -l root

COPY start_zookeeper /
COPY zookeeper-ready /

RUN yum install -y rsyslog java-11-openjdk-headless && \
    yum clean all && \
    rm -rf /var/cache/yum

RUN cd /tmp && \
    curl -k -s -O https://10.10.150.66/repository/xl/zookeeper-${ZOOKEEPER_VERSION}.tar.gz && \
    tar -C /opt -xvf zookeeper-${ZOOKEEPER_VERSION}.tar.gz && \
    ln -s zookeeper-${ZOOKEEPER_VERSION} /opt/zookeeper && \
    rm zookeeper-${ZOOKEEPER_VERSION}.tar.gz && \
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
