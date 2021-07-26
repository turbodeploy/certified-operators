#!/bin/bash

suffix=build
if [ -n "$1" ]; then
  suffix=${1}
fi

docker network rm build_default
docker stop build_db_1
docker rm build_db_1

cd $suffix
docker-compose up -d db

this=`basename "$(head /proc/1/cgroup)"`
docker network connect ${suffix}_default $this

docker-compose up -d consul
docker-compose up -d --force-recreate kafka1

sleep 10

docker-compose up -d kafka1
cd  ..
