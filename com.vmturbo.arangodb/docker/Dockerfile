FROM ubuntu:18.04
MAINTAINER Michael Borodiansky <michael.borodiansky@turbonomic.com>

ENV ARCHITECTURE amd64
ENV ARANGO_VERSION 3.3.22-1
ENV ARANGO_URL https://download.arangodb.com/arangodb33/xUbuntu_17.04
ENV ARANGO_PACKAGE arangodb3-${ARANGO_VERSION}_${ARCHITECTURE}.deb
ENV ARANGO_PACKAGE_URL ${ARANGO_URL}/${ARCHITECTURE}/${ARANGO_PACKAGE}

RUN apt-get -q -y update && apt-get -q -y --no-install-recommends upgrade && \
    apt-get install -q -y --no-install-recommends \
        libjemalloc1 \
        rsyslog \
        libsnappy1v5 \
        libssl1.0.0 \
        ca-certificates \
        pwgen \
        curl \
        python-flask \
    && \
    rm -rf /var/lib/apt/lists/* && \
	mkdir /docker-entrypoint-initdb.d && \
	curl -s -O ${ARANGO_PACKAGE_URL} &&       \
    (echo arangodb3 arangodb3/password password test | debconf-set-selections) && \
    (echo arangodb3 arangodb3/password_again password test | debconf-set-selections) && \
    DEBIAN_FRONTEND="noninteractive" dpkg -i ${ARANGO_PACKAGE} && \
    rm -rf /var/lib/arangodb3/* && \
    sed -ri \
# https://docs.arangodb.com/latest/Manual/Administration/Configuration/Endpoint.html
        -e 's!127\.0\.0\.1!0.0.0.0!g' \
# https://docs.arangodb.com/latest/Manual/Administration/Configuration/Logging.html
        -e 's!^(file\s*=).*!\1 -!' \
# run as arangodb:arangodb
        -e 's!^#\s*uid\s*=.*!uid = arangodb!' \
        -e 's!^#\s*gid\s*=.*!gid = arangodb!' \
        /etc/arangodb3/arangod.conf \
    && \
    DEBIAN_FRONTEND="noninteractive" apt-get purge -y --auto-remove ca-certificates && \
    DEBIAN_FRONTEND="noninteractive" apt-get purge -y --auto-remove curl && \
    rm -f ${ARANGO_PACKAGE}*

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
