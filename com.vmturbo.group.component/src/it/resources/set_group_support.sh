#!/bin/bash

function start_rsyslog() {
    docker run -d --name rsyslog$suffix \
    --mount type=volume,src=syslogdata,dst=/home/vmtsyslog,volume-driver=local \
    --mount type=volume,src=syslogdata_rotate,dst=/var/lib/logrotate,volume-driver=local \
    --mount type=volume,src=auditlogdata,dst=/var/log/turbonomic,volume-driver=local \
    turbonomic/syslog:latest
}

function start_consul() {
    docker run -d --name consul$suffix \
    -e 'CONSUL_BIND_INTERFACE=eth0' \
    -e 'constraint:node==/node-4/' \
    -e 'consul.standalone=true' \
    --mount type=volume,src=consuldata,dst=/consul/data,volume-driver=local \
    turbonomic/com.vmturbo.consul:latest vmt-server -client=0.0.0.0
}

function start_db() {
    docker run -d --name db$suffix \
    --mount type=volume,src=mysqldata,dst=/home/mysql/var/lib/mysql,volume-driver=local \
    --mount type=volume,src=mysql_log,dst=/home/mysql/var/log,volume-driver=local \
    --mount type=volume,src=mysql_var,dst=/home/mysql/var/run,volume-driver=local \
    --mount type=volume,src=mysql_tmp,dst=/home/mysql/tmp,volume-driver=local \
    -p 3306:3306 \
    turbonomic/db:latest
}

function start_zoo() {
    docker run -d --name zoo$suffix \
    -e 'ZOO_MY_ID=1' \
    -e 'ZOO_SERVERS=zoo$suffix' \
    --mount type=volume,src=zoo1-data,dst=/var/lib/zoo,volume-driver=local \
    --network kafka-net$suffix -e ALLOW_ANONYMOUS_LOGIN=yes \
    -p 2181:2182 \
    turbonomic/zookeeper:latest
}

function start_kafka() {
docker run -d --name kafka$suffix \
    -e 'BROKER_ID=1' \
    -e 'ALLOW_PLAINTEXT_LISTENER=yes' \
    -e 'KAFKA_CFG_ZOOKEEPER_CONNECT=zoo$suffix:2181' \
    -e 'KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092' \
    --mount type=volume,src=kafka1-log,dst=/home/kafka/data,volume-driver=local \
    --network kafka-net$suffix \
    -p 9092:9092 \
    bitnami/kafka:latest
}

suffix=IT
if [ -n "$1" ]; then
  suffix=${1}
fi

start_rsyslog
start_consul
start_db

docker network create kafka-net$suffix --driver bridge

start_zoo
start_kafka
