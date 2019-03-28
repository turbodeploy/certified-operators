FROM registry.access.redhat.com/rhel7
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

RUN groupadd -g 101 mysql && useradd -r -g 101 -s /bin/bash -u 101 mysql

ENV VERSION=10.1.38

RUN echo "[mariadb]" > /etc/yum.repos.d/MariaDB.repo && \
    echo "name = MariaDB-${VERSION}" >> /etc/yum.repos.d/MariaDB.repo && \
    echo "baseurl = http://yum.mariadb.org/${VERSION}/centos7-amd64" >> /etc/yum.repos.d/MariaDB.repo && \
    echo "gpgkey=https://yum.mariadb.org/RPM-GPG-KEY-MariaDB" >> /etc/yum.repos.d/MariaDB.repo && \
    echo "gpgcheck=1" >> /etc/yum.repos.d/MariaDB.repo && \
    yum install -y rsyslog MariaDB-server MariaDB-client net-tools && \
    rm -rf /var/run/mysqld && rm -rf /var/lib/mysql && \
    yum clean all && \
    rm -rf /var/cache/yum

# Copy the files where needed.
# The files are copied as root.
COPY entrypoint.sh /
COPY my.cnf /etc/mysql/
COPY change_buffer_pool_size.sh /
RUN chmod 755 /change_buffer_pool_size.sh

# rsyslog
ADD rsyslog.conf /etc

# Done with everything. Time to chown and chmod before turning off root.
RUN mkdir -p /var/lib/mysql &&\
    chown -R mysql:mysql /var/lib/mysql && \
    mkdir -p /var/lib/mysql/tmp && \
    chown -R mysql:mysql /var/lib/mysql/tmp && \
    mkdir -p /var/run/mysqld && \
    chown -R mysql:mysql /var/run/mysqld && \
    mkdir -p /var/log/mysql && \
    chown -R mysql:mysql /var/log/mysql && \
    chmod 755 /entrypoint.sh && passwd -l root

USER mysql

# The Database port
EXPOSE 3306

VOLUME ["/var/lib/mysql"]
ENTRYPOINT ["/entrypoint.sh"]
