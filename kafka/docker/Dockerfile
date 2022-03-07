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

# This is the release of openj9 to pull in.
ENV OPENJ9_URL=https://github.com/ibmruntimes/semeru11-binaries/releases/download/jdk-11.0.13%2B8_openj9-0.29.0/ibm-semeru-open-11-jre-11.0.13.8_0.29.0-1.x86_64.rpm

# This is the release of Kafka and Scala to pull in.
COPY kafka.profile /etc/kafka.profile
COPY start-kafka /

# Create a kafka user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The kafka-logs dir is used by Kafka to store data.
RUN groupadd -g 1000 kafka && useradd -m -r -d /home/kafka -g 1000 -s /bin/bash -u 101 kafka && \
    passwd -l root

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng gnutls && \
    dnf install -y $OPENJ9_URL && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN source /etc/kafka.profile && \
    cd /tmp && \
    curl -k -s -O http://apache.mirrors.pair.com/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -C /opt -xvf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    ln -s kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mkdir /opt/kafka/logs && \
    chown -R kafka /opt/kafka/logs && \
    mkdir $KAFKA_DATA_DIR && \
    chown -R kafka $KAFKA_DATA_DIR && \
    chmod a+x /start-kafka

# rsyslog
ADD rsyslog.conf /etc
ADD log4j.properties /opt/kafka/config

EXPOSE 9092

ENTRYPOINT ["/start-kafka"]

USER kafka
