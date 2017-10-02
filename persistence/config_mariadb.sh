#!/bin/bash

set -e

__mysql_config() {
  echo "Running the mysql_config function."
  mysql_install_db
  chown -R mysql:mysql /var/lib/mysql
  /usr/bin/mysqld_safe &
  sleep 5
}

__start_mysql() {
  printf "Running the start_mysql function.\n"
  USER="vmtplatform"
  PASS="vmturbo"
  mysqladmin -u root password "$PASS"
  mysql -uroot -p"$PASS" <<-EOF
    SET character_set_client = utf8;
	DELETE FROM mysql.user WHERE user = '$USER';
	FLUSH PRIVILEGES;
	CREATE USER '$USER'@'localhost' IDENTIFIED BY '$PASS';
	GRANT ALL PRIVILEGES ON *.* TO '$USER'@'localhost' WITH GRANT OPTION;
	CREATE USER '$USER'@'%' IDENTIFIED BY '$PASS';
	GRANT ALL PRIVILEGES ON *.* TO '$USER'@'%' WITH GRANT OPTION;
EOF

  pushd /srv/rails/webapps/persistence/db && ./initialize_all.sh && popd

  killall mysqld
  sleep 1
}

# Call all functions
DB_FILES=$(echo /var/lib/mysql/*)
DB_FILES="${DB_FILES#/var/lib/mysql/\*}"
DB_FILES="${DB_FILES#/var/lib/mysql/lost+found}"
if [ -z "$DB_FILES" ]; then
  printf "Initializing empty /var/lib/mysql...\n"
  __mysql_config
  __start_mysql
fi

# Don't run this again.
rm -f /scripts/config_mariadb.sh
