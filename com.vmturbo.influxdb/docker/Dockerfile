FROM ubuntu:18.04
MAINTAINER XL Components <XLComponents@turbonomic.com>

ENV ARCHITECTURE amd64
ENV INFLUXDB_VERSION 1.7.0
ENV INFLUXDB_URL https://dl.influxdata.com/influxdb/releases
ENV INFLUXDB_PACKAGE influxdb_${INFLUXDB_VERSION}_${ARCHITECTURE}.deb
ENV INFLUXDB_PACKAGE_URL ${INFLUXDB_URL}/${INFLUXDB_PACKAGE}
ENV INFLUX_CONF /etc/influxdb/influxdb.conf

RUN apt-get update -q -y && apt-get upgrade -q -y && \
    apt-get install -q -y --no-install-recommends \
        libjemalloc1 \
        rsyslog \
        ca-certificates \
        xz-utils \
        curl \
        python-flask \
        net-tools \
        vim-tiny \
    && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    curl -s -O ${INFLUXDB_PACKAGE_URL} && \
    DEBIAN_FRONTEND="noninteractive" dpkg -i ${INFLUXDB_PACKAGE} && \
    DEBIAN_FRONTEND="noninteractive" apt-get purge -y --auto-remove ca-certificates && \
    DEBIAN_FRONTEND="noninteractive" apt-get purge -y --auto-remove curl && \
    rm -f ${INFLUXDB_PACKAGE}*

# Initialization
RUN mkdir -p /var/lib/influxdb && \
    mkdir -p /home/influxdb/influxdb-dump && \
    chown -R influxdb /var/lib/influxdb && \
    chown -R influxdb /home/influxdb

# rsyslog
ADD rsyslog.conf /etc

# Set up volumes for the database directory and the dump directory
VOLUME ["/var/lib/influxdb", "/home/influxdb/influxdb-dump"]

COPY entrypoint.sh /entrypoint.sh
COPY influx_dump_scheduler.py /influx_dump_scheduler.py
ENTRYPOINT ["/entrypoint.sh"]
RUN chmod 755 /entrypoint.sh && \
    chmod 755 /influx_dump_scheduler.py
RUN passwd -l root
USER influxdb

# InfluxDB port
EXPOSE 8086

# InfluxDB backup-and-restore port
EXPOSE 8088
