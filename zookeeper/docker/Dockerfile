FROM ubuntu:17.10
MAINTAINER Pavel Baranchikov <pavel.baranchikov@turbonomic.com>

# This is the release of Zookeeper to pull in.
ENV ZOOKEEPER_VERSION=3.4.10

# Create a zookeeper user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The /tmp/zookeeper dir is used by Zookeeper transaction log to store state.
RUN addgroup zookeeper && \
    adduser --system --disabled-password -home /home/zookeeper --ingroup zookeeper --disabled-login --shell /bin/bash zookeeper

COPY start_zookeeper /

RUN apt-get update -q -y && apt-get upgrade -q -y && \
    apt-get install -q -y openjdk-8-jdk iproute2 wget bash rsyslog && \
    cd /tmp && \
    wget --quiet --no-check-certificate https://10.10.150.66/repository/xl/zookeeper-${ZOOKEEPER_VERSION}.tar.gz && \
    tar -C /opt -xvf zookeeper-${ZOOKEEPER_VERSION}.tar.gz && \
    ln -s zookeeper-${ZOOKEEPER_VERSION} /opt/zookeeper && \
    rm zookeeper-${ZOOKEEPER_VERSION}.tar.gz && \
    mkdir /tmp/zookeeper && \
    chown -R zookeeper /tmp/zookeeper && \
    mkdir /var/lib/zoo && \
    chown -R zookeeper /var/lib/zoo && \
    chmod a+x /start_zookeeper && \
    apt-get purge -y --auto-remove wget unzip && \
    rm -rf /var/lib/apt/lists/* && apt-get clean


# rsyslog
ADD rsyslog.conf /etc
ADD log4j.properties /opt/zookeeper/conf

EXPOSE 2181

ENTRYPOINT ["/start_zookeeper"]

USER zookeeper
