ARG hydra_version=v1.11.10
ARG hydra_user=472
ARG hydra_group=1000
FROM registry.access.redhat.com/ubi8 AS python
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

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

RUN dnf install -y --nobest python3-devel
RUN dnf install -y --nobest gcc && \
    dnf clean all && \
    python3 -m pip install --upgrade pip

WORKDIR /python-fetch/
RUN chown -R hydra:hydra /python-fetch

# Install common libs that every component has
RUN curl https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -s -o /etc/yum.repos.d/epel-release-latest-8.noarch.rpm && \
    dnf install -y --nobest rsyslog procps-ng && \
    # clean up
    dnf clean all

ADD rsyslog.conf /etc

# prepare entry-point script
ADD entrypoint.sh /
COPY install.sh .

RUN chmod +x /entrypoint.sh ./install.sh && \
    ./install.sh -d -b /usr/bin hydra $hydra_version && \
    rm ./install.sh

USER hydra

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt --target=/python-fetch/pylib

FROM registry.access.redhat.com/ubi8 AS hydra
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

ARG hydra_version
ARG hydra_user
ARG hydra_group

RUN groupadd -g $hydra_group hydra && useradd -r -g $hydra_group -s /bin/bash -u $hydra_user hydra

RUN dnf install -y --nobest python3-devel

COPY --from=python /python-fetch/pylib /pylib

# Install common libs that every component has
RUN curl https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -s -o /etc/yum.repos.d/epel-release-latest-8.noarch.rpm && \
    dnf install -y --nobest rsyslog procps-ng && \
    # clean up
    dnf clean all

ADD rsyslog.conf /etc

# prepare entry-point script
ADD entrypoint.sh /
COPY install.sh .

RUN chmod +x /entrypoint.sh ./install.sh && \
    ./install.sh -d -b /usr/bin hydra $hydra_version && \
    rm ./install.sh

# Make sure that python can find installed libraries and grpc bindings
ENV PYTHONPATH="/pylib:/grpc:/util"

# Where we expect the turbonomic configmap properties to be mounted by the helm chart
ENV TURBO_CONFIG_PROPERTIES_FILE=/etc/turbonomic/properties.yaml

ENV DB_PATHS_CONFIG="/etc/hydra/hydra.ini" \
  HYDRA_PATHS_DATA="/hydra-data" \
  HYDRA_PATHS_HOME="/tmp/hydra/home" \
  HYDRA_PATHS_LOGS="/var/log/hydra" \
  HYDRA_PATHS_PLUGINS="/tmp/hydra/plugins" \
  HYDRA_PATHS_PROVISIONING="/tmp/etc/hydra/provisioning" \
  HYDRA_PATH="$HYDRA_PATHS_HOME/bin/hydra-server"

COPY hydra.ini /etc/hydra/hydra.ini

# copy utility scripts
COPY util /util/
RUN chmod +x /util/*.py /util/*.sh

ENTRYPOINT [ "/entrypoint.sh" ]