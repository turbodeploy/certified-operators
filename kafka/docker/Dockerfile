FROM alpine
MAINTAINER Pavel Baranchikov <pavel.baranchikov@turbonomic.com>

# This is the release of Kafka and Scala to pull in.
COPY kafka.profile /etc/kafka.profile

# Create a kafka user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
# The kafka-logs dir is used by Kafka to store data.
RUN addgroup kafka && adduser -S -D -h /home/kafka -s /bin/bash -G kafka kafka && passwd -l root

COPY start-kafka /

RUN source /etc/kafka.profile && \
    apk update && \
    apk add --no-cache rsyslog python2 openjdk8-jre-base ca-certificates bash util-linux wget && \
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
    apk del wget unzip 

# rsyslog
ADD rsyslog.conf /etc
ADD log4j.properties /opt/kafka/config

EXPOSE 9092

ENTRYPOINT ["/start-kafka"]

USER kafka
