FROM ubuntu:17.04
MAINTAINER Michael Borodiansky <michael.borodiansky@turbonomic.com>

RUN apt-get update -q -y && apt-get upgrade -q -y && \
apt-get install -q -y --no-install-recommends ca-certificates rsyslog libaio1 mariadb-server mariadb-client && \
rm -rf /var/run/mysqld && rm -rf /var/lib/mysql

# Copy the files where needed.
# The files are copied as root.
COPY entrypoint.sh /
COPY my.cnf /etc/mysql/

# rsyslog
ADD rsyslog.conf /etc

# We done wverything, time to chown and chmod before turning off root.
RUN mkdir -p /var/lib/mysql &&\
chown -R mysql:mysql /var/lib/mysql && \
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
