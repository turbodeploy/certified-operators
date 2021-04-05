#!/bin/bash

# This script creates the directories needed to enable local storage class in Kubernetes (if they are not already there).
# It is safe to re-run this script.

mkdir -p /opt/local/data/api-certs
mkdir -p /opt/local/data/api
mkdir -p /opt/local/data/auth
mkdir -p /opt/local/data/consul-data
mkdir -p /opt/local/data/kafka-log
mkdir -p /opt/local/data/zookeeper-data
mkdir -p /opt/local/data/rsyslog-syslogdata
mkdir -p /opt/local/data/rsyslog-auditlogdata
mkdir -p /opt/local/data/topology-processor
mkdir -p /opt/local/data/prometheus-alertmanager
mkdir -p /opt/local/data/prometheus-server
mkdir -p /opt/local/data/graphstate-datacloud-graph
