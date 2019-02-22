#!/bin/bash

set -e

# Change the temp dir location for mariadb
MY_CNF="/var/lib/docker/volumes/docker_mysqldata/_data/my.cnf"
TMPDIR=/var/lib/mysql/tmp
# We need to bring up the db container so that we can set
# up the permissions for the tmpdir as mysql user
docker-compose -f /etc/docker/docker-compose.yml up -d db
docker exec docker_db_1 mkdir -p $TMPDIR
docker exec docker_db_1 chmod 700 $TMPDIR
docker exec docker_db_1 chown -R mysql:mysql $TMPDIR
docker-compose -f /etc/docker/docker-compose.yml stop db
sed  -i 's/= \/tmp/= \/var\/lib\/mysql\/tmp/' $MY_CNF
