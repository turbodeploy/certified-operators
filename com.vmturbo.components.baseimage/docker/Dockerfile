FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="baseimage" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="baseimage" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

# This is the release of openj9 to pull in.
ENV OPENJ9_URL=https://github.com/ibmruntimes/semeru11-binaries/releases/download/jdk-11.0.13%2B8_openj9-0.29.0/ibm-semeru-open-11-jre-11.0.13.8_0.29.0-1.x86_64.rpm

# Install rsyslog and OpenJDK java-11 packages
RUN dnf install -y rsyslog procps-ng python2 net-tools xz gnutls && \
    dnf install -y $OPENJ9_URL && \
    dnf update -y krb5-libs \
    dnf clean all && rm -rf /var/cache/dnf

COPY entrypoint.sh collect_diags.sh diags.py terminate.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /collect_diags.sh && chmod 755 /terminate.sh && \
    chmod 755 /diags.py

# add the swagger UI
COPY swagger /swagger/

# rsyslog
ADD rsyslog.conf /etc

# Create a user which is not allowed to logon in the system.
RUN useradd -m -r -d /home/turbonomic -g 50 -s /bin/bash -u 102 turbonomic && \
    passwd -l root

USER turbonomic

RUN chmod 755 /home/turbonomic && mkdir -p /home/turbonomic/data

ENTRYPOINT ["/entrypoint.sh"]
