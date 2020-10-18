#!/bin/bash

# Wait until rsyslog container is up.
# It it is not, we can loose some valuable logs information related to the component startup
if [ -z "$LOG_TO_STDOUT" ]; then
    try_number=0
    while ! (echo ""> /dev/tcp/rsyslog/2514) ; do
        sleep 1;
        try_number=`expr $try_number + 1`
        echo "Waiting for rsyslog to accept connections";
        if [ "$try_number" -ge 30 ]; then
             echo "Failed to access rsyslog daemon to 30 seconds. Exiting..."
            exit 1;
        fi
    done;
    echo "Successfully reached rsyslog. Starting the DB component..."
fi

DEFAULT_MYSQL_CONF=/etc/mysql/my.cnf
MYSQL_CONF=/var/lib/mysql/my.cnf
USER=`/usr/bin/id -nu`

if [ -z "$RETRY_LIMIT" ]; then
  RETRY_LIMIT=30
fi

# rsyslog and touch the log
touch /var/log/mysql/mariadb-slow.log /var/log/mysql/error.log
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

# write stdin to log
function log() { logger --tag mariadb -u /tmp/log.sock; }
# function to log a message
function logmsg() { echo $(date -u +'%Y-%m-%d %H:%M:%S') "$@" | log; }
# function to shut down mysqld
function mysqldown() { mysqladmin -u root -pvmturbo -S /var/run/mysqld/mysqld.sock --wait shutdown 2>&1 | log; }

logmsg "*** DB Component Startup"
if [ ! -d "/var/run/mysqld" ]; then
    mkdir -p /var/run/mysqld 2>&1 | log
fi

copy_mysql_default_conf_file () {
    logmsg "Copying default DB config. file from $DEFAULT_MYSQL_CONF to $MYSQL_CONF"
    # backslash suppresses any potential alias
    if [ "$USER" == "root" ]; then
      \su mysql -c "cp -p $DEFAULT_MYSQL_CONF $MYSQL_CONF" 2>&1 | logger --tag mariadb -u /tmp/log.sock
     else
      \cp $DEFAULT_MYSQL_CONF $MYSQL_CONF 2>&1 | logger --tag mariadb -u /tmp/log.sock
    fi
}

if [ ! -d "/var/lib/mysql/mysql" ]; then

    logmsg "*** MariadB initial start"

    if [ "$USER" == "root" ]; then
      /usr/bin/mysql_install_db --user=mysql --defaults-file=$DEFAULT_MYSQL_CONF 2>&1 | log
     else
      /usr/bin/mysql_install_db --auth-root-socket-user=$USER --defaults-file=$DEFAULT_MYSQL_CONF 2>&1 | log
    fi

    copy_mysql_default_conf_file

    # Start mysqld in background
    /usr/sbin/mysqld --defaults-file=$MYSQL_CONF --skip-networking 2>&1 | log &
    for i in `seq 1 $RETRY_LIMIT`
    do
        echo "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'vmturbo' WITH GRANT OPTION; \
              GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' IDENTIFIED BY 'vmturbo' WITH GRANT OPTION; \
              FLUSH PRIVILEGES; " | /usr/bin/mysql -S /var/run/mysqld/mysqld.sock -u $USER &>/dev/null
        if [ "$?" -eq 0 ]; then
            logmsg '+++ MariaDB privileges grant successful.'
            break
        fi
        logmsg '*** MariaDB init process in progress...'
        sleep 1
    done

    # Terminate the initial mysqld
    if ! mysqldown ; then
        logmsg '--- MariaDB init process failed. Unable to terminate daemon.'
        exit 1
    fi
    # Check the status
    if (( "$i" == $RETRY_LIMIT )); then
        logmsg '--- MariaDB init process failed. Exhausted number of attempts.'
        exit 1
    fi

    logmsg '+++ MariaDB init process successful.'
else
    logmsg "*** MariaDB restart"
    # The mysql conf is copied to the $MYSQL_CONF location during DB initilization.
    # But if it is missing(maybe due to upgrade or the file was deleted), then copy it from
    # the default location.
    if [ ! -f $MYSQL_CONF ]; then
      copy_mysql_default_conf_file
    fi

    # Start mysqld in background for any upgrade processing that may be needed.
    # Networking is disabled so other components can't connect.
    /usr/sbin/mysqld --defaults-file=$MYSQL_CONF --user=$USER --datadir=/var/lib/mysql --lc-messages-dir=/usr/share/mysql --skip-networking 2>&1 | log &
    # Upgrade mysql database if the database server was updated
    logmsg "*** Awaiting connection to perform any needed data upgrades"
    for (( i = 1; i > 0; i++ ))
    do
      if [[ -e /var/run/mysqld/mysqld.sock ]] ; then
        /usr/bin/mysql_upgrade -S /var/run/mysqld/mysqld.sock -u root -pvmturbo 2>&1 | log
        if [ "${PIPESTATUS[0]}" -eq 0 ]; then
          logmsg '+++ MariaDB databases are up-to-date.'
          break
        fi
      fi
      # If the db server is not running, reset to the default config if the server does not start, exit and retry
      pid=$(ps -e -o pid,args | grep [m]ysqld | awk '{print $1}')
      if [ -z "pid" ]; then
          logmsg '--- MariaDB upgrade process failed. Background mysqld is not running. Resetting config'
          copy_mysql_default_conf_file
          exit 1
      fi
      # Server not yet accepting connections. Make initial retries with 1-second
      # waits, then assume something more time-consuming is going on, like crash recovery,
      # and switch to unlimited retries with 1-minute waits.
      if ((i == $RETRY_LIMIT)); then
        # log when we switch wait modes
        logmsg "--- MariaDB is slow starting up, perhaps performing recovery after a crash."
        logmsg "--- This script will wait forever; if this is unexpected, please manually kill this script and investigate"
      fi
      if ((i < RETRY_LIMIT)); then sleep 1; else sleep 60; fi
    done

    # Terminate the initial mysqld
    if ! mysqldown; then
        logmsg '--- MariaDB upgrade process failed. Unable to terminate daemon.'
    fi

    logmsg '+++ MariaDB upgrade process successful.'
fi

# At this point the DB should be stopped, regardless of the branch taken above

if [ "$USER" == "root" ]; then
  su mysql -c "/customize_my_cnf.sh $MYSQL_CONF" 2>&1 | log
else
  /customize_my_cnf.sh $MYSQL_CONF 2>&1 | log
fi

# Start the database server, with network connections available
logmsg "+++ Starting DB Server for use by components"
exec /usr/sbin/mysqld --defaults-file=$MYSQL_CONF > >(logger --tag mariadb -u /tmp/log.sock) 2>&1
