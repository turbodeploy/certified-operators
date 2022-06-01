ARG hydra_version=v1.11.7
FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

ARG hydra_version

# Required OpenShift Labels
LABEL name="hydra" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="hydra" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Copy Turbo license file and Apache 2 license file
COPY licenses /licenses

# Add user and group beforehand, so that rpm won't create its own with differen uid/gid.
RUN groupadd -g 1000 hydra && useradd -r -g 1000 -s /bin/bash -u 102 hydra && \
    passwd -l root

# Install common libs that every component has
RUN curl https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -s -o /etc/yum.repos.d/epel-release-latest-8.noarch.rpm && \
    dnf install -y --nobest rsyslog procps-ng && \
    # clean up
    dnf clean all

ADD rsyslog.conf /etc

# prepare entry-point script
ADD entrypoint.sh /
RUN chmod +x /entrypoint.sh

COPY install.sh .
RUN chmod +x ./install.sh

# copy over hydra executable
RUN ./install.sh -d -b /usr/bin hydra $hydra_version && \
    rm ./install.sh

USER hydra

ENTRYPOINT [ "/entrypoint.sh" ]