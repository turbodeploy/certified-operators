# increased the version because we were hitting a bug in the latest version of mariadb that was
# available in ubuntu 17:10
# strangely enough, ubuntu 18:10 has a version lower than the 17:10 (not sure why)
# old version: 10.1.30-MariaDB-0ubuntu0.17.10.1
# new version: 10.1.29-MariaDB-6ubuntu2
FROM ubuntu:18.04
MAINTAINER Michael Borodiansky <michael.borodiansky@turbonomic.com>

RUN apt-get update -q -y && apt-get upgrade -q -y && \
apt-get install -q -y --no-install-recommends ca-certificates rsyslog libaio1 mariadb-server mariadb-client && \
rm -rf /var/run/mysqld && rm -rf /var/lib/mysql

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
