FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="arangodb" \
      vendor="Turbonomic" \
      version="7" \
      release="1" \
      summary="arangodb" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

ENV ARANGO_VERSION 3.6.4-1.0
ENV ARANGO_URL https://download.arangodb.com/arangodb36/RPM/x86_64/arangodb3-${ARANGO_VERSION}.x86_64.rpm
ENV OPENSSL_URL http://mirror.centos.org/centos/8/AppStream/x86_64/os/Packages/compat-openssl10-1.0.2o-3.el8.x86_64.rpm

# The systemd-coredump userid 999 would be the owner of the /var/lib/arangodb3 directory after the ubi8 update
# We should have created arangodb user with an explicit userid instead of it picking 999
RUN userdel systemd-coredump

# Install rsyslog and other packages
RUN dnf install -y https://download.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm && \
    dnf install -y rsyslog procps-ng python2 jemalloc pwgen gnutls && \
# Install flask, for use by arango_dump_restore script
    python2.7 -m pip install flask && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN mkdir /docker-entrypoint-initdb.d && \
    dnf install -y ${OPENSSL_URL} && \
    dnf install -y ${ARANGO_URL} && \
    rm -rf /var/lib/arangodb3/* &&\
    sed -ri \
# https://docs.arangodb.com/latest/Manual/Administration/Configuration/Endpoint.html
        -e 's!127\.0\.0\.1!0.0.0.0!g' \
# https://docs.arangodb.com/latest/Manual/Administration/Configuration/Logging.html
        -e 's!^(file\s*=).*!\1 -!' \
# run as arangodb:arangodb
        -e 's!^#\s*uid\s*=.*!uid = arangodb!' \
        -e 's!^#\s*gid\s*=.*!gid = arangodb!' \
        /etc/arangodb3/arangod.conf

# Recreated the delete user now with a different userid, probably 998
RUN useradd -r -d / -g 997 -s /sbin/nologin systemd-coredump

COPY arangod.conf /etc/arangodb3/
RUN chown arangodb:arangodb /etc/arangodb3/arangod.conf

# Initialization
# The presense of /tmp/init_007 indicates the the fact that arangoDB went through the setup,
# but never started yet. Will be removed on first startup.
RUN mkdir -p /var/lib/arangodb3 && mkdir -p /var/lib/arangodb3-apps && mkdir -p /home/arangodb/arangodb-dump && \
    chown -R arangodb /var/lib/arangodb3 && chown -R arangodb /var/lib/arangodb3-apps && chown -R arangodb /home/arangodb && \
    ARANGODB_DEFAULT_ROOT_PASSWORD="root" /usr/sbin/arango-init-database

# rsyslog
ADD rsyslog.conf /etc

# retain the database directory, the Foxx Application directory and the arango dump directory
VOLUME ["/var/lib/arangodb3", "/var/lib/arangodb3-apps", "/home/arangodb/arangodb-dump"]

# Copy scripts to dump/restore data during upgrades
COPY mmfiles-dump.sh mmfiles-restore.sh wait-during-start.sh /
RUN chmod +x /mmfiles-dump.sh /mmfiles-restore.sh /wait-during-start.sh

COPY docker-entrypoint.sh /entrypoint.sh
COPY arango_dump_restore.py /

ENTRYPOINT ["/entrypoint.sh"]
RUN chmod 755 /entrypoint.sh && chmod 755 /arango_dump_restore.py
RUN passwd -l root
USER arangodb

# ArangoDB
EXPOSE 8529

# Dump/Restore webservice
EXPOSE 8599
CMD ["arangod"]
