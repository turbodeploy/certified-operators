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
    export WORKER_PROCESSES=2
fi

if [ "$SSL_PROTOCOLS" == "" ]; then
    export SSL_PROTOCOLS='TLSv1.2'
fi

if [ "$SSL_CIPHERS" == "" ]; then
    export SSL_CIPHERS='EECDH+ECDSA+AESGCM EECDH+aRSA+AESGCM EECDH+ECDSA+SHA384 EECDH+ECDSA+SHA256 EECDH+aRSA+SHA384 EECDH+aRSA+SHA256 EECDH+aRSA+RC4 EECDH EDH+aRSA RC4 !aNULL !eNULL !LOW !3DES !MD5 !EXP !PSK !SRP !DSS !RC4'
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

if [ "$UI" == "" ]; then
    export UI=unset
fi

if [ "$BLOCK_REMOTE_PROBES" == "" ]; then
    export BLOCK_REMOTE_PROBES='FALSE'
fi

# In order to utilize the same feature flag for nginx as the platform for enabling the probe authentication,
# we are obtaining the values of enableProbeAuth and enableMandatoryProbeAuth from the mounted properties
# configmap as the nested feature flag value is not accessible from the nginx deployment in our current configuration.
if [ -f "/etc/turbonomic/properties.yaml" ]; then
    test $(grep enableProbeAuth /etc/turbonomic/properties.yaml | wc -l) -lt 2 || echo "Error: Multiple instances of enableProbeAuth specified"
    export ENABLE_PROBE_AUTH=$( grep enableProbeAuth /etc/turbonomic/properties.yaml | awk 'BEGIN { FS = ":" } { print $2 }' | tr -d "[:blank:]" )
fi

if [ "$ENABLE_PROBE_AUTH" == "" ]; then
    export ENABLE_PROBE_AUTH='FALSE'
fi

if [ -f "/etc/turbonomic/properties.yaml" ]; then
    test $(grep enableMandatoryProbeAuth /etc/turbonomic/properties.yaml | wc -l) -lt 2 || echo "Error: Multiple instances of enableMandatoryProbeAuth specified"
    export ENABLE_MANDATORY_PROBE_AUTH=$( grep enableMandatoryProbeAuth /etc/turbonomic/properties.yaml | awk 'BEGIN { FS = ":" } { print $2 }' | tr -d "[:blank:]" )
fi

if [ "$ENABLE_MANDATORY_PROBE_AUTH" == "" ]; then
    export ENABLE_MANDATORY_PROBE_AUTH='FALSE'
fi

if [ "$AUTH" == "" ]; then
    export AUTH='auth.turbonomic.svc.cluster.local'
fi

if [ "$HYDRA_PUBLIC" == "" ]; then
    export HYDRA_PUBLIC='hydra-public.turbonomic.svc.cluster.local'
fi

if [ "$CLIENT_NETWORK" == "" ]; then
    export CLIENT_NETWORK='client-network.turbonomic.svc.cluster.local'
fi

# Allow IPv4 and IPv6 formats.
# Validation checks characters are alphanumeric : / . for specifying ip.
if [ "$WHITE_LIST_IPS" != "" ]; then
    allowString=""
    ipList=($WHITE_LIST_IPS)
    for ip in "${ipList[@]}"
    do
        if [[ $ip =~ ^[a-zA-Z0-9:/.]+$ ]]; then
            allowString="${allowString} allow ${ip};"
        else
            echo "${ip} is invalid"
        fi
    done
    export WHITE_LIST_IPS="${allowString} deny all;"
fi

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

ENV_VARS=(
    '${API}' '${UI}' '${GRAFANA}' '${TOPOLOGY}' '${DNS_RESOLVER}' '${WORKER_PROCESSES}'
    '${WORKER_CONNECTIONS}' '${SSL_PROTOCOLS}' '${SSL_CIPHERS}' '${DISABLE_HTTPS_REDIRECT}'
    '${BLOCK_REMOTE_PROBES}' '${WHITE_LIST_IPS}' '${AUTH}' '${HYDRA_PUBLIC}' '${CLIENT_NETWORK}'
    '${ENABLE_PROBE_AUTH}' '${ENABLE_MANDATORY_PROBE_AUTH}'
)
mkdir -p /tmp/nginx/includes
envsubst "${ENV_VARS[*]}" < /etc/nginx/nginx.conf.template > /tmp/nginx/nginx.conf
sed -i "s=/etc/nginx/includes/=/tmp/nginx/includes/=" /tmp/nginx/nginx.conf


find /etc/nginx/includes -type f  | while read conf; do
    envsubst "${ENV_VARS[*]}" < "$conf" > /tmp/nginx/includes/"$(basename "$conf")"
    sed -i "s=/etc/nginx/includes/=/tmp/nginx/includes/=" /tmp/nginx/includes/"$(basename "$conf")"
done

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
    echo "Finished with certs" | ${LOGGER_COMMAND}
    sed -i "s=/etc/nginx/certs=/tmp/certs=" /tmp/nginx/nginx.conf
fi

start_nginx() {
    echo "Starting nginx" 2>&1 | ${LOGGER_COMMAND}
    exec nginx -c /tmp/nginx/nginx.conf -p /var/log/nginx > >($LOGGER_COMMAND) 2>&1
}

start_nginx
