FROM quay.io/skupper/site-controller:1.0.1
MAINTAINER "Billy O'Connell <billy.oconnell@turbonomic.com>"

# Required OpenShift Labels
LABEL name="skupper" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="skupper" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

RUN microdnf install -y shadow-utils passwd && \
    groupadd -g 1000 skupper && useradd -m -r -d /home/skupper -g 1000 -s /bin/bash -u 101 skupper \
    && passwd -l root

# Install rsyslog
RUN microdnf install -y rsyslog procps-ng gnutls && \
    microdnf clean all && \
    rm -rf /var/cache/dnf

ADD rsyslog.conf /etc
ADD rsyslog-entrypoint.sh /rsyslog-entrypoint.sh
RUN chmod +x /rsyslog-entrypoint.sh

ENV COMMAND=/app/site-controller
ENV COMPONENT_NAME=skupper-site-controller

# default values
ENV RSYSLOG_SERVER_ADDRESS=rsyslog
ENV RSYSLOG_SERVER_PORT=2514

ENTRYPOINT [ "/rsyslog-entrypoint.sh" ]

USER skupper
