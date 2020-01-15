#!/bin/bash

DEFAULT_MYSQL_CONF=/etc/mysql/my.cnf
MYSQL_CONF=/var/lib/mysql/my.cnf
USER=`/usr/bin/id -nu`
RETRY_LIMIT=30

# rsyslog and touch the log
touch /var/log/mysql/mariadb-slow.log
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

if [ ! -d "/var/run/mysqld" ]; then
    mkdir -p /var/run/mysqld 2>&1 | logger -u /tmp/log.sock
fi

copy_mysql_default_conf_file () {
    echo "Copying default DB config. file from $DEFAULT_MYSQL_CONF to $MYSQL_CONF" | logger --tag mariadb -u /tmp/log.sock
    # backslash suppresses any potential alias
    \su mysql -c "cp $DEFAULT_MYSQL_CONF $MYSQL_CONF" 2>&1 | logger --tag mariadb -u /tmp/log.sock
}

if [ ! -d "/var/lib/mysql/mysql" ]; then

    echo "Initializing mariadb " | logger --tag mariadb -u /tmp/log.sock

    /usr/bin/mysql_install_db --user=mysql --datadir=/var/lib/mysql --defaults-file=$DEFAULT_MYSQL_CONF --basedir=/usr 2>&1 | logger --tag mariadb -u /tmp/log.sock

    copy_mysql_default_conf_file

    # Start mysqld in background
    /usr/sbin/mysqld --defaults-file=$MYSQL_CONF --user=mysql --datadir=/var/lib/mysql --lc-messages-dir=/usr/share/mysql --skip-networking 2>&1 | logger --tag mariadb -u /tmp/log.sock &
    for i in `seq 1 $RETRY_LIMIT`
    do
        echo "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'vmturbo' WITH GRANT OPTION; \
              GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' IDENTIFIED BY 'vmturbo' WITH GRANT OPTION; \
              FLUSH PRIVILEGES; " | /usr/bin/mysql -S /var/run/mysqld/mysqld.sock -u $USER &>/dev/null
        if [ "$?" -eq 0 ]; then
            echo '+++ MariaDB privileges grant successful.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
            break
        fi
        echo '*** MariaDB init process in progress...' 2>&1 | logger --tag mariadb -u /tmp/log.sock
        sleep 1
    done
    pid=$(ps -e -o pid,args | grep [m]ysqld | awk '{print $1}')
    # Terminate the initial mysqld
    if ! kill -s TERM "$pid" || ! wait "$pid"; then
        echo '--- MariaDB init process failed. Unable to terminate daemon.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
        exit 1
    fi
    # Check the status
    if [ "$i" -eq $RETRY_LIMIT ]; then
        echo '--- MariaDB init process failed. Exhausted number of attempts.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
        exit 1
    fi

    echo '+++ MariaDB init process successful.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
else
    # The mysql conf is copied to the $MYSQL_CONF location during DB initilization.
    # But if it is missing(maybe due to upgrade or the file was deleted), then copy it from
    # the default location.
    if [ ! -f $MYSQL_CONF ]; then
        copy_mysql_default_conf_file
    fi

    # Start mysqld in background
    /usr/sbin/mysqld --defaults-file=$MYSQL_CONF --user=mysql --datadir=/var/lib/mysql --lc-messages-dir=/usr/share/mysql --skip-networking 2>&1 | logger --tag mariadb -u /tmp/log.sock &
    # Upgrade mysql database if the database server was updated
    for i in `seq 1 $RETRY_LIMIT`
    do
        sleep 1
        /usr/bin/mysql_upgrade -S /var/run/mysqld/mysqld.sock -u root -pvmturbo 2>&1 | logger --tag mariadb -u /tmp/log.sock
        if [ "${PIPESTATUS[0]}" -eq 0 ]; then
            echo '+++ MariaDB databases are up-to-date.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
            break
        fi
    done
    # Check the status
    if [ "$i" -eq $RETRY_LIMIT ]; then
        echo '--- MariaDB upgrade process failed. Exhausted number of attempts.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
        exit 1
    fi

    pid=$(ps -e -o pid,args | grep [m]ysqld | awk '{print $1}')
    # Terminate the initial mysqld
    if ! kill -s TERM "$pid" || ! wait "$pid"; then
        echo '--- MariaDB upgrade process failed. Unable to terminate daemon.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
    fi

    echo '+++ MariaDB upgrade process successful.' 2>&1 | logger --tag mariadb -u /tmp/log.sock
fi

/customize_my_cnf.sh $MYSQL_CONF  2>&1 | logger --tag mariadb -u /tmp/log.sock

# Start the database server
exec /usr/sbin/mysqld --defaults-file=$MYSQL_CONF --user=mysql --datadir=/var/lib/mysql --lc-messages-dir=/usr/share/mysql > >(logger --tag mariadb -u /tmp/log.sock) 2>&1
