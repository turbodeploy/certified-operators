FROM registry.access.redhat.com/rhel7
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

ENV ARCHITECTURE x86_64
ENV ARANGO_VERSION 3.4.6-1.1
ENV ARANGO_URL https://download.arangodb.com/arangodb34/RPM
ENV ARANGO_PACKAGE arangodb3-${ARANGO_VERSION}.${ARCHITECTURE}.rpm
ENV ARANGO_PACKAGE_URL ${ARANGO_URL}/${ARCHITECTURE}/${ARANGO_PACKAGE}

RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && \
        yum update -y && \
        yum install -y \
        jemalloc \
        rsyslog \
        snappy \
        libssl1.0.0 \
        ca-certificates \
        pwgen \
        curl \
        python-flask \
    yum clean all && \
    rm -rf /var/cache/yum

RUN mkdir /docker-entrypoint-initdb.d && \
	curl -k -s -O ${ARANGO_PACKAGE_URL} && \
    rpm -i ${ARANGO_PACKAGE} && \
    rm -rf /var/lib/arangodb3/* &&\
    sed -ri \
# https://docs.arangodb.com/latest/Manual/Administration/Configuration/Endpoint.html
        -e 's!127\.0\.0\.1!0.0.0.0!g' \
# https://docs.arangodb.com/latest/Manual/Administration/Configuration/Logging.html
        -e 's!^(file\s*=).*!\1 -!' \
# run as arangodb:arangodb
        -e 's!^#\s*uid\s*=.*!uid = arangodb!' \
        -e 's!^#\s*gid\s*=.*!gid = arangodb!' \
        /etc/arangodb3/arangod.conf && \
    rm -f ${ARANGO_PACKAGE}*

COPY arangod.conf /var/lib/arangodb3/.

# Initialization
# The presense of /tmp/init_007 indicates the the fact that arangoDB went through the setup,
# but never started yet. Will be removed on first startup.
RUN mkdir -p /var/lib/arangodb3 && mkdir -p /var/lib/arangodb3-apps && mkdir -p /home/arangodb/arangodb-dump && chown -R arangodb /var/lib/arangodb3 && chown -R arangodb /var/lib/arangodb3-apps && \
chown -R arangodb /home/arangodb && ARANGODB_DEFAULT_ROOT_PASSWORD="root" /usr/sbin/arango-init-database && touch /tmp/init_007 && chown arangodb /tmp/init_007

# rsyslog
ADD rsyslog.conf /etc

# retain the database directory, the Foxx Application directory and the arango dump directory
VOLUME ["/var/lib/arangodb3", "/var/lib/arangodb3-apps", "/home/arangodb/arangodb-dump"]

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
