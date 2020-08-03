#!/bin/bash
# Makes sure the first command in the piped series fails the entire thing.
set -eo pipefail

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

# if WORKER_CONNECTIONS is set, it will be used to set the worker_connections property in the
# generated nginx.conf file. This controls the number of connections available per worker process.
# This defaults to 512 (the nginx default value), if WORKER_CONNECTIONS is not set.
if [ "$WORKER_CONNECTIONS" == "" ]; then
    export WORKER_CONNECTIONS=512
fi

# if $WORKER_PROCESSES is set, it will be used to set the worker_processes property in the
# generated nginx.conf file. This controls the number of worker processes spawned by nginx.
# This defaults to 1 (the nginx default value), if $WORKER_PROCESSES is not set.
if [ "$WORKER_PROCESSES" == "" ]; then
    export WORKER_PROCESSES=1
fi

if [ "$DNS_RESOLVER" == "" ]; then
    export DNS_RESOLVER=`cat /etc/resolv.conf | grep "nameserver" | awk '{print $2}' | tr '\n' ' '`
fi

# If the Grafana DNS name is not provided, we set it to a default string ("unset") which we will
# check for in the nginx config.
#
# This needs to be in sync with the value in nginx.conf.template.
if [ "$GRAFANA" == "" ]; then
    export GRAFANA=unset
fi

envsubst '${API} ${GRAFANA} ${TOPOLOGY} ${DNS_RESOLVER} ${WORKER_PROCESSES} ${WORKER_CONNECTIONS}' < /etc/nginx/nginx.conf.template > /tmp/nginx.conf

# If LOG_TO_STDOUT is defined in the environment, tee the output so that it is also logged to stdout.
# This is generally desirable in a development setup where you want to see the output on the console when
# starting a component, but not in production where we do not want logging to be captured by Docker
# and consume disk space (Docker JSON log driver captures and saves them then docker logs shows them).
# In a production environment, get the logs from the rsyslog component instead.
if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag ${instance_id} -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag ${instance_id} -u /tmp/log.sock)"
fi

echo "Configuring" $WORKER_PROCESSES "and" $WORKER_CONNECTIONS "connections per process." 2>&1 | ${LOGGER_COMMAND}

# Generate a certificate if there isn't one yet
if [ ! -f "/etc/nginx/certs/tls.crt" ] | [ ! -f "/etc/nginx/certs/tls.key" ]; then
    # generate a cert
    echo "Generating certs" 2>&1 | ${LOGGER_COMMAND}
    rm -f /tmp/certs/*
    mkdir -p /tmp/certs
    pushd /tmp/certs
    openssl req -newkey rsa:2048 -x509 -sha256 -days 365 -nodes -keyout tls.key -new -out tls.crt -subj /CN=localhost -reqexts SAN -extensions SAN \
      -config <(printf '[ req ]\ndistinguished_name	= req_distinguished_name\n[ req_distinguished_name ]\ncountryName = US\n[SAN]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n extendedKeyUsage = serverAuth, clientAuth, emailProtection\n keyUsage=nonRepudiation, digitalSignature, keyEncipherment')
    popd
    sed -i "s/etc\/nginx\/certs/tmp\/certs/" /tmp/nginx.conf
fi

start_nginx() {
    echo "Starting nginx" 2>&1 | ${LOGGER_COMMAND}
    exec nginx -c /tmp/nginx.conf -p /var/log/nginx > >($LOGGER_COMMAND) 2>&1
}

start_nginx
