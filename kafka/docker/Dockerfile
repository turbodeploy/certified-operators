FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# This is the release of Kafka and Scala to pull in.
COPY kafka.profile /etc/kafka.profile
COPY start-kafka /

# Create a kafka user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The kafka-logs dir is used by Kafka to store data.
RUN groupadd -g 1000 kafka && useradd -m -r -d /home/kafka -g 1000 -s /bin/bash -u 101 kafka && \
    passwd -l root

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng java-11-openjdk-headless && \
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
