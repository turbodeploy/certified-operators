# This docker file is capable of building an image that contains multiple versions of the
# grafana server for use in different scenarios, if needed. The following ARG specifies the
# versions as a list of <name>:<version> pairs. 
# Note: Any versions released privately have distinct properties. Include <name> of private
# release grafana version(s) as a list of strings.
ARG grafana_versions="embedded-reporting:8.5.15 saas-reporting:7.5.10"
ARG private_releases=("embedded-reporting")

# The grafana.ini config file is, in our case, created by the helm chart responsible for the
# grafana deployment. It is mounted as a read-only volume mounted at /etc/grafana/grafana.ini.
# Adjustments to grafana parameters can be made in either of two places:
#
# * in the CR file by setting `spec.grafana."grafana.ini".section.param`, where `section` names
#   the config file section containing the parameter, and `param` is the parmaeter name
# * In the overall chart's `values.yaml` file, similar structures appear under
#   `grafana."grafana.ini.defaults"`. Each such structure provides values for some subset of the
#   available parameters.

# When the grafana.ini file is constructed, values in the CR file have the highest precedence,
# followed by values in a `grafana.ini.defaults` block whose key is the current `deploymentMode`
# value (or the "_default" block if `deploymentMode` is unspecified). Lowest priority values come
# from a block named "_base" in that same `grafana.ini.defaults` object.

# Currently a single renderer version works in the configured grafana versions; this dockerfile will
# need to be adjusted for per-grafana-version renderer versions if that does not continue to be true
ARG grafana_img_renderer_version=v3.0.1

# We set up the grafana user in each dockerfile section, so the numeric group and user ids must
# be consistent across all sections. The user id must also correspond to the value specified in
# the helm charts ('runAsUser' in values.yaml).
ARG grafana_user=472
ARG grafana_group=1000

FROM registry.access.redhat.com/ubi8 AS python
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

ARG grafana_versions
ARG grafana_img_renderer_version
ARG grafana_group
ARG grafana_user

# This dockerfile section installs python and the libraries required for the image. We will copy
# the libraries into the final image but will leave behind all else created here, which includes
# the gcc library needed for some library builds. We don't attempt to carry over artifacgts of the
# python install itself, since attempting to identify them is too error-prone. Instead, we
# re-install python in the final image.
RUN groupadd -g $grafana_group grafana && useradd -r -g $grafana_group -s /bin/bash -u $grafana_user grafana

# do base python install as its own step so we can get the resulting layer directly from the cache
# when this is repeated in final build
RUN dnf install -y --nobest python3-devel
RUN dnf install -y --nobest gcc && \
    dnf update -y krb5-libs \
    dnf clean all && \
    python3 -m pip install --upgrade pip

# install required libraries, using run-time grafana user & group ids, to be copied in the final image
WORKDIR /python-fetch/
RUN chown -R grafana:grafana /python-fetch

USER grafana

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt --target=/python-fetch/pylib

FROM registry.access.redhat.com/ubi8 AS grafana
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

ARG grafana_versions
ARG grafana_img_renderer_version
ARG grafana_group
ARG grafana_user
ARG private_releases

# This dockerfile section installs all the configured grafana versions from tarballs, to be
# copied into the final image

RUN groupadd -g $grafana_group grafana && useradd -r -g $grafana_group -s /bin/bash -u $grafana_user grafana

# As of grafana v 8.3.9, embedded reporting runs on a private release (See OM-87097). 
# When the version is replaced with an open source version, remove "embedded-reporting" from the private_releases list.
# (Defaults to enterprise release path, unless 'name' is in the private releases list)

RUN dnf install -y wget && \
    for version in $grafana_versions ; do \
       name=${version%:*}; \
       v=${version#*:}; \
       path="oss/release" && [[ ${private_releases[*]} =~ ${name} ]]  && path="private-releases/oss"; \
       wget https://dl.grafana.com/$path/grafana-$v.linux-amd64.tar.gz \
           -nv --show-progress --progres=bar:force:noscrolls:giga; \
    done
RUN dnf update -y krb5-libs


WORKDIR /grafana-fetch

# All the tarballs are expanded with the configured grafana user/group
RUN chown -R grafana:grafana /grafana-fetch
USER grafana

RUN for version in $grafana_versions; do \
      v=${version#*:}; \
      tar -zxf /grafana-$v.linux-amd64.tar.gz; \
    done

FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# This dockerfile section creates the final image, pulling needed bits from the prior sections
# and setting up environment variables with needed grafana path overrides. The entrypoint script
# is responsible for selecting the grafana version that will be used in the running container,
# by creating symlinks in /tmp.

# Required OpenShift Labels
LABEL name="grafana" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="grafana" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Copy Turbo license file.
COPY licenses /licenses

ARG grafana_versions
ARG grafana_img_renderer_version
ARG grafana_user
ARG grafana_group

RUN groupadd -g $grafana_group grafana && useradd -r -g $grafana_group -s /bin/bash -u $grafana_user grafana

# install python now so we get the cached layer from docker cache (since so far we're identical to
# --from=python section)

RUN dnf install -y --nobest python3-devel

# lock root password
RUN passwd -l root

# import files populated in prior dockerfile sections
COPY --from=python /python-fetch/pylib /pylib
COPY --from=grafana /grafana-fetch/ /grafana/

# copy python classes generated in our maven build from protobuf definitions, so that our python
# code can make use of needed grpc methods from turbonomic components
COPY grpc /grpc

COPY CentOS-AppStream.repo /etc/yum.repos.d/
COPY CentOS-Base.repo /etc/yum.repos.d/
# The name and file path of the key needs to match the ones configured in the CentOS .repo files \
RUN curl http://mirror.centos.org/centos/RPM-GPG-KEY-CentOS-Official -s -o /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-Official
    # Install common libs that every component has
RUN curl https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -s -o /etc/yum.repos.d/epel-release-latest-8.noarch.rpm && \
    dnf install -y --nobest rsyslog procps-ng && \
    dnf update -y krb5-libs \
    # clean up
    dnf clean all

COPY rsyslog.conf /etc

# point grafana at its files - some of these paths won't exist in the final image and will be
# established by the entrypoint script, e.g. by using symlinks
ENV GF_PATHS_CONFIG="/etc/grafana/grafana.ini" \
  GF_PATHS_DATA="/grafana-data" \
  GF_PATHS_HOME="/tmp/grafana/home" \
  GF_PATHS_LOGS="/var/log/grafana" \
  GF_PATHS_PLUGINS="/tmp/grafana/plugins" \
  GF_PATHS_PROVISIONING="/tmp/etc/grafana/provisioning" \
  GRAFANA_PATH="$GF_PATHS_HOME/bin/grafana-server"

# copy in provisioned dashboards and datasources
COPY dashboards /grafana-dashboards/
COPY datasources /grafana-datasources/

# set up directories required by grafana at runtime. Where these will differ based on grafana
# version it will be up to the entrypoint script to make selections using symlinks.
RUN mkdir -p /tmp/etc/grafana/provisioning/{dashboards,datasources,plugins,notifiers} \
             /grafana-plugins /tmp/grafana/plugins  \
             /grafana-data /grafana-ini /var/log/grafana && \
    chown -R grafana:grafana /tmp/etc/grafana /tmp/grafana /grafana-dashboards /grafana-plugins \
             /grafana-datasources /grafana-data /grafana-ini /var/log/grafana

COPY grafana.ini /etc/grafana/grafana.ini

# copy utility scripts
COPY util /util/
RUN chmod +x /util/*.py /util/*.sh

# create symlinks pointing at grafana versions by version name rather than version number
RUN for version in $grafana_versions; do \
        name=${version%:*}; \
        v=${version#*:}; \
        ln -s -f /grafana/grafana-$v /grafana-$name-runtime; \
    done

# prepare entry-point script
COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

# Make sure that python can find installed libraries and grpc bindings
ENV PYTHONPATH="/pylib:/grpc:/util"

# Where we expect the turbonomic configmap properties to be mounted by the helm chart
ENV TURBO_CONFIG_PROPERTIES_FILE=/etc/turbonomic/properties.yaml

USER grafana

# many of our dashboards are maintained as yaml... replace them with json
RUN find /grafana-dashboards/*/ -type f -iname '*.yaml' -print0 | xargs -0 /util/yaml2json.py

# install plugins we rely on, either for Embedded Reporting or Saas Reporting. This is the union
# of all plugins required in various deployments; the entrypoint script is responsible to configure
# only the plugins required in a given deployment.
# We use the first configured grafana version in order to run the cli to install plugins
RUN cli_version="${grafana_versions% *}" && cli_version="${cli_version%:*}"  && \
    /grafana-$cli_version-runtime/bin/grafana-cli \
        --pluginsDir /grafana-plugins \
        --pluginUrl https://grafana.com/api/plugins/marcusolsson-treemap-panel/versions/0.9.1/download \
        plugins install marcusolsson-treemap-panel


# This is our python script to manage the server, responding to newly uploaded licenses, SMTP
# config changes in turbonomic, etc.
COPY grafana_mgr.py /

EXPOSE 3000

STOPSIGNAL SIGTERM

ENTRYPOINT [ "/entrypoint.sh" ]
