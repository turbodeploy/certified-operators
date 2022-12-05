FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="db" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="db" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

RUN groupadd -g 101 mysql && useradd -r -g 101 -s /bin/bash -u 101 mysql

ENV VERSION=10.5.16
ENV BOOST_URL=http://10.10.150.66/repository/xl/boost-program-options-1.66.0-14.fc29.x86_64.rpm

RUN echo "[mariadb]" > /etc/yum.repos.d/MariaDB.repo && \
    echo "name = MariaDB-${VERSION}" >> /etc/yum.repos.d/MariaDB.repo && \
    #echo "baseurl = https://yum.mariadb.org/${VERSION}/rhel8-amd64/" >> /etc/yum.repos.d/MariaDB.repo && \
    echo "baseurl = https://archive.mariadb.org/mariadb-${VERSION}/yum/rhel8-amd64/" >> /etc/yum.repos.d/MariaDB.repo && \
    echo "gpgkey=https://yum.mariadb.org/RPM-GPG-KEY-MariaDB" >> /etc/yum.repos.d/MariaDB.repo && \
    echo "gpgcheck=1" >> /etc/yum.repos.d/MariaDB.repo && \
    dnf install -y ${BOOST_URL} && \
    dnf install -y --setopt=tsflags=nodocs --disablerepo "rhel-8-for-x86_64-appstream-rpms" rsyslog hostname procps-ng MariaDB-server net-tools gnutls && \
    dnf update -y krb5-libs \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Copy the files where needed.
# The files are copied as root.
COPY entrypoint.sh /
COPY my.cnf /etc/mysql/

# rsyslog
ADD rsyslog.conf /etc

# Done with everything. Time to chown and chmod before turning off root.
RUN rm -rf /var/run/mysqld && rm -rf /var/lib/mysql && \
    /usr/bin/sed -i "/^  chown \$user \"$pamtooldir/ s/^/#/" /usr/bin/mysql_install_db && \
    mkdir -p /var/lib/mysql && chown -R mysql:mysql /var/lib/mysql && \
    mkdir -p /var/lib/mysql/tmp && chown -R mysql:mysql /var/lib/mysql/tmp && \
    mkdir -p /var/run/mysqld && chown -R mysql:mysql /var/run/mysqld && \
    mkdir -p /var/log/mysql && chown -R mysql:mysql /var/log/mysql && \
    chmod 755 /entrypoint.sh && passwd -l root

USER mysql

# The Database port
EXPOSE 3306

VOLUME ["/var/lib/mysql"]
ENTRYPOINT ["/entrypoint.sh"]
