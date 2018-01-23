FROM ubuntu:17.10
MAINTAINER Pavel Baranchikov <pavel.baranchikov@turbonomic.com>

SHELL ["/bin/bash", "-c"]
# This is the release of Kafka and Scala to pull in.
COPY kafka.profile /etc/kafka.profile

# Create a kafka user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The kafka-logs dir is used by Kafka to store data.
RUN addgroup kafka && \
    adduser --system --disabled-password -home /home/kafka --ingroup kafka --disabled-login --shell /bin/bash kafka

COPY start-kafka /

RUN source /etc/kafka.profile && \
    apt-get update -q -y && apt-get upgrade -q -y && \
    apt-get install -q -y openjdk-8-jdk iproute2 wget bash rsyslog && \
    cd /tmp && \
    wget -q --no-check-certificate -O kafka.tgz  https://10.10.150.66/repository/xl/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -C /opt -xvf kafka.tgz && \
    ln -s kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm kafka.tgz && \
    mkdir /opt/kafka/logs && \
    chown -R kafka /opt/kafka/logs && \
    mkdir $KAFKA_DATA_DIR && \
    chown -R kafka $KAFKA_DATA_DIR && \
    chmod a+x /start-kafka && \
    apt-get purge -y --auto-remove wget unzip && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

# rsyslog
ADD rsyslog.conf /etc
ADD log4j.properties /opt/kafka/config

EXPOSE 9092

ENTRYPOINT ["/start-kafka"]

USER kafka
