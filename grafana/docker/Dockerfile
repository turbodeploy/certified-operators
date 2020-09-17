ARG grafana_version=7.1.1

FROM registry.access.redhat.com/ubi8 AS builder
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

ARG grafana_version

WORKDIR /grafana-fetch

RUN dnf install -y wget && \
    wget https://dl.grafana.com/enterprise/release/grafana-enterprise-$grafana_version.linux-amd64.tar.gz && \
    tar -zxvf grafana-enterprise-$grafana_version.linux-amd64.tar.gz


FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

ARG grafana_version

# Required OpenShift Labels
LABEL name="grafana" \
      vendor="Turbonomic" \
      version="7" \
      release="1" \
      summary="grafana" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

ENV GF_PATHS_CONFIG="/etc/grafana/grafana.ini" \
  GF_PATHS_DATA="/var/lib/grafana" \
  GF_PATHS_HOME="/grafana" \
  GF_PATHS_LOGS="/var/log/grafana" \
  GF_PATHS_PLUGINS="/grafana/plugins" \
  GF_PATHS_PROVISIONING="/etc/grafana/provisioning" \
  GRAFANA_PATH="/grafana/bin/grafana-server" \
  # Make sure that python can find the grpc bindings
  PYTHONPATH="/grpc" \
  GF_RENDERER_PLUGIN_CHROME_BIN="/usr/bin/chromium-browser"

# Add user and group beforehand, so that rpm won't create its own with differen uid/gid.
RUN groupadd -g 1000 grafana && useradd -r -g 1000 -s /bin/bash -u 102 grafana && \
passwd -l root

COPY --from=builder /grafana-fetch/grafana-$grafana_version $GF_PATHS_HOME

COPY CentOS-AppStream.repo /etc/yum.repos.d/
COPY CentOS-Base.repo /etc/yum.repos.d/

RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm && \
    # The name and file path of the key needs to match the ones configured in the CentOS .repo files.
    curl http://mirror.centos.org/centos/RPM-GPG-KEY-CentOS-Official > /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-Official
RUN dnf search chrom && \
    dnf install -y --nobest rsyslog procps python3-devel gcc udev chromium && \
    dnf clean all
RUN python3 -m pip install --upgrade pip
RUN $GF_PATHS_HOME/bin/grafana-cli \
    --pluginsDir "$GF_PATHS_PLUGINS" \
    --pluginUrl https://github.com/grafana/grafana-image-renderer/releases/latest/download/plugin-linux-x64-glibc-no-chromium.zip \
    plugins install grafana-image-renderer

COPY rsyslog.conf /etc
# The grpc folder is populated by Maven with unzipped python bindings for XL gRPC services.
COPY grpc /grpc
COPY entrypoint.sh /

RUN chown -R grafana:grafana $GF_PATHS_HOME && \
    mkdir -p "$GF_PATHS_PROVISIONING/datasources" \
             "$GF_PATHS_PROVISIONING/dashboards" \
             "$GF_PATHS_PROVISIONING/plugins" \
             "$GF_PATHS_PROVISIONING/notifiers" \
             "$GF_PATHS_LOGS" \
             "$GF_PATHS_PLUGINS" \
             "$GF_PATHS_DATA" && \
    cp $GF_PATHS_HOME/conf/sample.ini $GF_PATHS_CONFIG && \
    chown -R grafana:grafana "$GF_PATHS_DATA" "$GF_PATHS_LOGS" "$GF_PATHS_PLUGINS" "$GF_PATHS_PROVISIONING" && \
    chmod -R 777 "$GF_PATHS_DATA" "$GF_PATHS_LOGS" "$GF_PATHS_PLUGINS" "$GF_PATHS_PROVISIONING" && \
    # Forward the Grafana data log to stdout.
    ln -sf /dev/stdout $GF_PATHS_LOGS/grafana.log && \
    chmod 755 /entrypoint.sh

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY grafana_mgr.py /

EXPOSE 3000

USER grafana

STOPSIGNAL SIGTERM

ENTRYPOINT [ "/entrypoint.sh" ]