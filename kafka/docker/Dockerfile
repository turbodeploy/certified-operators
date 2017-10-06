FROM ubuntu:17.04
MAINTAINER Pavel Baranchikov <pavel.baranchikov@turbonomic.com>

# This is the release of Kafka and Scala to pull in.
ENV KAFKA_VERSION=0.11.0.1
ENV SCALA_VERSION=2.11

# Create a kafka user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The /tmp/kafka-logs dir is used by Kafka to store state.
RUN addgroup kafka && \
    adduser --system --disabled-password -home /home/kafka --ingroup kafka --disabled-login --shell /bin/bash kafka

COPY start-kafka /

RUN apt-get update -q -y && apt-get upgrade -q -y && \
    apt-get install -q -y openjdk-8-jdk iproute2 wget bash rsyslog && \
    cd /tmp && \
    wget -q -O kafka.tgz  http://mirror.linux-ia64.org/apache/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -C /opt -xvf kafka.tgz && \
    ln -s kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm kafka.tgz && \
    mkdir /opt/kafka/logs && \
    chown -R kafka /opt/kafka/logs && \
    mkdir /tmp/kafka-logs && \
    chown -R kafka /tmp/kafka-logs && \
    chmod a+x /start-kafka && \
    apt-get purge -y --auto-remove wget unzip && \
    rm -rf /var/lib/apt/lists/* && apt-get clean


# rsyslog
ADD rsyslog.conf /etc
ADD log4j.properties /opt/kafka/config

EXPOSE 9092

ENTRYPOINT ["/start-kafka"]

USER kafka
