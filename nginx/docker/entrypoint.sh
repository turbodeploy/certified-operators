#!/bin/bash
# Makes sure the first command in the piped series fails the entire thing.
set -eo pipefail

# rsyslog
rm -f /tmp/rsyslog.pid; /usr/sbin/rsyslogd -f /etc/rsyslog.conf -i /tmp/rsyslog.pid

if [ "$DNS_RESOLVER" == "" ]; then
    export DNS_RESOLVER=`cat /etc/resolv.conf | grep "nameserver" | awk '{print $2}' | tr '\n' ' '`
fi
envsubst '${API} ${TOPOLOGY} ${DNS_RESOLVER}' < /home/nginx/conf/nginx.conf.template > /home/nginx/conf/nginx.conf

# If LOG_TO_STDOUT is defined in the environment, tee the output so that it is also logged to stdout.
# This is generally desirable in a development setup where you want to see the output on the console when
# starting a component, but not in production where we do not want logging to be captured by Docker
# and consume disk space (Docker JSON log driver captures and saves them then docker logs shows them).
# In a production environment, get the logs from the rsyslog component instead.
if [[ -z ${LOG_TO_STDOUT} ]]; then
  export LOGGER_COMMAND="logger --tag nginx-${instance_id} -u /tmp/log.sock"
else
  export LOGGER_COMMAND="eval tee >(logger --tag nginx-${instance_id} -u /tmp/log.sock)"
fi

mkdir -p /tmp/certs

# extract the cert and key from a pkcs12 file, if one was provided
if [[ -f "/etc/nginx/certs/key.pkcs12" ]]; then
    # extract a cert and key from the pkcs12 file
    echo "Extracting cert from key.pkcs12" 2>&1 | ${LOGGER_COMMAND}
    if [ -z "$KEYPASS" ]; then
        KEYPASS="jumpy-crazy-experience"
    fi
    pushd /etc/nginx/certs
    openssl pkcs12 -in key.pkcs12 -nocerts -nodes -out /tmp/certs/cert.key -passin pass:${KEYPASS}
    openssl pkcs12 -in key.pkcs12 -nokeys -out /tmp/certs/cert.pem -passin pass:${KEYPASS}
    popd
fi

# Generate a certificate if there isn't one yet
if [[ ! -f "/tmp/certs/cert.pem" ]]; then
    # generate a cert
    echo "Generating certs" 2>&1 | ${LOGGER_COMMAND}
    rm -f /tmp/certs/*
    pushd /tmp/certs
    openssl genrsa -out cert.key 2048
    openssl req -new -sha256 -key cert.key -out csr.csr -subj '/CN=turbonomic'
    openssl req -x509 -sha256 -days 3650 -key cert.key -in csr.csr -out cert.pem
    popd
fi

start_nginx() {
    echo "Starting nginx" 2>&1 | ${LOGGER_COMMAND}
    exec nginx -c /home/nginx/conf/nginx.conf > >($LOGGER_COMMAND) 2>&1
}

start_nginx
