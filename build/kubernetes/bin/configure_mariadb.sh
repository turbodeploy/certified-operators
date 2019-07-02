#!/bin/bash

# Script to configure MariaDB on the VM

set -euo pipefail

source libs.sh

SRC_MY_CNF="/opt/turbonomic/kubernetes/etc/my.cnf"
MY_CNF="/etc/my.cnf.d/server.cnf"

configure_buffer_pool() {
    local MYSQL_CONF=${1}
	TOTAL_MEM_BYTES=$(cat /proc/meminfo  | grep MemTotal | awk  '{print $2*1024}')
    GB_BYTES=$((1024*1024*1024))
    DB_MEM_PCT_FOR_BUFFER_POOL=75

	if [ $TOTAL_MEM_BYTES -le $((32*GB_BYTES)) ];then
		MEM_FOR_DB=$((4 * GB_BYTES))
	elif [ $TOTAL_MEM_BYTES -le $((64*GB_BYTES)) ];then
		MEM_FOR_DB=$((8*GB_BYTES))
    else
		MEM_FOR_DB=$((12*GB_BYTES)) # 12GB
    fi

    BUFFER_POOL_SIZE_MB=$(( (MEM_FOR_DB * DB_MEM_PCT_FOR_BUFFER_POOL) / (100*1024*1024) ))
	echo "Total Memory: $(( (TOTAL_MEM_BYTES)/(1024*1024) )) MB"
	echo "Changing Innodb buffer pool size to: ${BUFFER_POOL_SIZE_MB} MB"

	sudo sed -i 's/innodb_buffer_pool_size.*/innodb_buffer_pool_size = '$BUFFER_POOL_SIZE_MB'M/' $MYSQL_CONF
}

# Check if mariadb is installed
yum list installed Mariadb-server
if [ "$?" -ne 0 ];
then
    log_msg "Mariadb server package is not installed. Aborting..."
    exit 1
fi

sudo systemctl stop mariadb.service
sudo cp -f $SRC_MY_CNF $MY_CNF

mariadb_tmp_dir=$(grep tmpdir $MY_CNF  | awk '{print $NF}')
mariadb_data_dir=$(grep datadir $MY_CNF  | awk '{print $NF}')
if [ ! -d "$mariadb_data_dir" ]; then
    sudo ln -s /usr/share/mysql /share
    log_msg "Initializing mariadb "
    sudo mkdir -p -m 700 $mariadb_tmp_dir
    sudo chown -R mysql:mysql $mariadb_tmp_dir
    sudo mysql_install_db --defaults-file=$MY_CNF --user=mysql --basedir="/"
fi

#Configure innodb buffer pool
configure_buffer_pool $MY_CNF

log_msg "Starting mariadb service"
sudo systemctl start mariadb.service
if [ "$?" -ne 0 ];
then
    log_msg "Failed to start Mariadb server. Aborting..."
    exit 1
fi

# Setup permissions for XL access
for i in `seq 1 5`
    do
        echo "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'vmturbo' WITH GRANT OPTION; \
              GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' IDENTIFIED BY 'vmturbo' WITH GRANT OPTION; \
              FLUSH PRIVILEGES; " | /usr/bin/mysql  -uroot
        if [ "$?" -eq 0 ]; then
            log_msg '+++ MariaDB privileges granted successful.'
            break
        fi
        log_msg '*** MariaDB init process in progress...'
        sleep $i
done

log_msg "Successfully configured mariadb."

