FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="zookeeper" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="zookeeper" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

# This is the release of Zookeeper to pull in.
ENV ZOOKEEPER_VERSION=3.6.3

# Create a zookeeper user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The /tmp/zookeeper dir is used by Zookeeper transaction log to store state.
RUN groupadd -g 1000 zookeeper && useradd -m -r -d /home/zookeeper -g 1000 -s /bin/bash -u 101 zookeeper && \
passwd -l root

COPY start_zookeeper /
COPY zookeeper-ready /

# Install rsyslog and OpenJDK java-11 packages
# Unfortunately, our build system was built before BuildKit was available.
# Additionally, we use spotify's docker-maven-plugin for triggering the docker builds.
# Support for this plugin was dropped before BuildKit was available.
# For now we use a solution that's good enough for a developer to get stuff running on their laptop.
# When we want to support multi-architecture builds, we will need to migrate to a different maven
# plugin in OM-93669.
RUN case $(uname -m) in \
      "aarch64")  \
        ARCH=aarch64 ;; \
      *) \
        ARCH=x86_64;  ;; \
    esac \
    && OPENJ9_URL=https://github.com/ibmruntimes/semeru11-binaries/releases/download/jdk-11.0.13%2B8_openj9-0.29.0/ibm-semeru-open-11-jre-11.0.13.8_0.29.0-1.$ARCH.rpm \
    && echo "downloading jdk from $OPENJ9_URL" \
    && dnf install -y rsyslog procps-ng python2 gnutls \
    && dnf install -y $OPENJ9_URL \
    && dnf clean all \
    && rm -rf /var/cache/dnf

ARG ZOOKEEPER_TAR=apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz

RUN cd /tmp && \
    curl -k -s -O https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/${ZOOKEEPER_TAR} && \
    #curl -k -s -O http://apache.mirrors.pair.com/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/${ZOOKEEPER_TAR} && \
    tar -C /opt -xvf ${ZOOKEEPER_TAR} && \
    ln -s apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && \
    rm ${ZOOKEEPER_TAR} && \
    mkdir /tmp/zookeeper && \
    chown -R zookeeper /tmp/zookeeper && \
    mkdir /var/lib/zoo && \
    chown -R zookeeper /var/lib/zoo && \
    chmod a+x /start_zookeeper && \
    chmod a+x /zookeeper-ready && \
    # Remove old netty components
    rm /opt/zookeeper/lib/netty-*.* && \
    # Remove old log4j 1.x components
    rm /opt/zookeeper/lib/slf4j-*.* && \
    rm /opt/zookeeper/lib/log4j-*.* && \
    # Remove old jackson jars (vulnerable)
    rm /opt/zookeeper/lib/jackson-*.* && \
    # Remove old jetty jars (vulnerable)
    rm /opt/apache-zookeeper-3.6.3-bin/lib/jetty-*.*

# Add reload4j components
ADD lib/* /opt/zookeeper/lib/

# rsyslog
ADD rsyslog.conf /etc
ADD log4j.properties /opt/zookeeper/conf

EXPOSE 2181

ENTRYPOINT ["/start_zookeeper"]

USER zookeeper
