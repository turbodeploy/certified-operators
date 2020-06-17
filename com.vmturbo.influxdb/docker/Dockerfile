FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="influxdb" \
      vendor="Turbonomic" \
      version="7" \
      release="1" \
      summary="influxdb" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

# Add user and group beforehand, so that rpm won't create its own with differen uid/gid.
RUN groupadd -g 1000 influxdb && useradd -r -g 1000 -s /bin/bash -u 102 influxdb && \
passwd -l root

# Install rsyslog and influxdb packages
RUN dnf install -y rsyslog procps-ng openssl python2 gettext gnutls
RUN echo "[influxdb]" > /etc/yum.repos.d/influxdb.repo && \
    echo "name=InfluxDB Repository" >> /etc/yum.repos.d/influxdb.repo && \
    echo "baseurl=https://repos.influxdata.com/rhel/7/x86_64/stable/" >> /etc/yum.repos.d/influxdb.repo && \
    echo "gpgkey=https://repos.influxdata.com/influxdb.key" >> /etc/yum.repos.d/influxdb.repo && \
    echo "gpgcheck=1" >> /etc/yum.repos.d/influxdb.repo && \
    echo "enabled=1" >> /etc/yum.repos.d/influxdb.repo && \
    dnf install -y influxdb && \
    dnf clean all && \
    rm -rf /var/cache/dnf

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

RUN chmod 755 /entrypoint.sh && \
    chmod 755 /influx_dump_scheduler.py

# InfluxDB port
EXPOSE 8086

# InfluxDB backup-and-restore port
EXPOSE 8088

USER influxdb

ENTRYPOINT ["/entrypoint.sh"]
