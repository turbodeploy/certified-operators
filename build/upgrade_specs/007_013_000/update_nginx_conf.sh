#!/bin/bash

set -e

# Update the nginx.conf to the latest version.
NGINX_CONF_TMPL_SRC="/tmp/nginx.conf.template"
NGINX_CONF_TMPL_DEST="/var/lib/docker/volumes/docker_nginx-home/_data/conf/nginx.conf.template"

cat > $NGINX_CONF_TMPL_SRC << 'EOF'
# include any additional config files for the main http block
include /etc/nginx/userconf/*.conf;

# keep process alive so docker container doesn't exit
daemon off;

worker_processes  1;

error_log  /var/log/nginx/error.log warn;
pid        /var/run/nginx.pid;

events {
    worker_connections 256;
}

http {
    include       /etc/nginx/mime.types;
    default_type  application/octet-stream;

    log_format  main  '[$time_local] $remote_addr - $remote_user "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for"';

    log_format upstream_ws '[$time_local] $remote_addr - $remote_user - $server_name to: $upstream_addr: $request upstream_response_time $upstream_response_time msec $msec request_time $request_time';

    access_log  /var/log/nginx/access.log  main;

    # turn off proxy buffering due to read-only fs
    proxy_buffering off;

    sendfile        on;
    #tcp_nopush     on;

    keepalive_timeout  300; # may need to increase for idle websockets

    # These two should be the same or nginx will start writing
    #  large request bodies to temp files
    client_body_buffer_size 10m;
    client_max_body_size    10m;


    ## gzip settings
    # enable gzip
    gzip on;
    # tells proxies to cache both gzipped and regular versions of a resource
    gzip_vary on;
    #  do not compress anything smaller than this size
    gzip_min_length 1024;
    # compress data even for clients that are connecting via proxies
    gzip_proxied any;
    #  disable compression for Internet Explorer versions 4-6
    gzip_disable "msie6";
    # gzip also 1.0 requests. this is because nginx is proxying requests on http 1.0 by default (and otherwise they will not be gzipped)
    gzip_http_version 1.0;
    # compression level, from 1 to 9
    gzip_comp_level 5;
    # enable gzip responses for specific MIME types
    gzip_types text/plain text/css application/json text/javascript application/x-javascript application/javascript text/xml application/xml application/xml+rss font/truetype font/opentype application/vnd.ms-fontobject image/svg+xml;


    # for passing the _crsf header for spring security
    underscores_in_headers on;

    # force nginx to re-resolve dns entries every 5 seconds, otherwise it will cache old entries
    # and fail to detect or recover from upstream server outages. To do this, we need to explicitly
    # set the resolver to the docker name resolver and set the valid time appropriately.
    resolver $DNS_RESOLVER valid=5s ipv6=off;

    # for websocket upgrades
    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    server {
        listen 8000;

        location / {
            return 301 https://$host$request_uri;
        }

        location = /metrics {
            set $api_servers $API;
            proxy_pass         http://$api_servers:8080/metrics;
            proxy_set_header   Host $http_host;
        }

        # include any additional server config files for the insecure port
        include /etc/nginx/userconf/insecure/*.conf;
    }

    server {
        listen 8443 ssl http2;
        ssl on;
        ssl_certificate /tmp/certs/cert.pem;
        ssl_certificate_key /tmp/certs/cert.key;
        ssl_protocols TLSv1 TLSv1.1 TLSv1.2;

        # proxying default paths
        location ~ (/$|/index|/app|/vmturbo(?!/messages)|/rest|/api|/cgi-bin|/swagger) {
            # we are using variables for the upstream servers instead of an upstream entry because
            # of the DNS caching problem -- only nginx plus will dynamically resolve the upstream
            # servers
            set $api_servers $API;
            proxy_pass         http://$api_servers:8080;
            proxy_set_header   Host $http_host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 10s;
            proxy_read_timeout 600s;
            proxy_intercept_errors on;

            # Ensure the proxy-side redirect from API gets rewritten to the gateway address
            proxy_set_header   X-Forwarded-Proto $scheme;
            proxy_redirect     http://$http_host/ $scheme://$http_host/;

            # Send 503 "service unavailable" on gateway issues, where API may be down or unavailable
            error_page 502 503 504 =503 /maintenance/;

            access_log  /var/log/nginx/access.log  upstream_ws;
        }

        location /assets {
            set $api_servers $API;
            proxy_pass         http://$api_servers:8080;
            proxy_set_header   Host $http_host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            access_log off;
        }

        # In a production environment the assets and docs are in a resource bundle, and there
        # are no requests to this path. But in a local development environment with an
        # un-compressed UI we need to support this route or else the UI won't work.
        location /doc {
            set $api_servers $API;
            proxy_pass         http://$api_servers:8080;
            proxy_set_header   Host $http_host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            access_log off;
        }

        # For proxying websockets
        location = /ws/messages {
            set $api_servers $API;

            # For proxying of websockets to API SSL port
            #proxy_pass         https://$api_servers:9443/ws/messages;
            # For proxying to non-SSL API
            proxy_pass        http://$api_servers:8080/ws/messages;

            proxy_set_header   Host $http_host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header   X-Forwarded-Host $server_name;

            proxy_set_header   X-Forwarded-Proto $scheme;
            proxy_set_header   X-NginX-Proxy true;
            proxy_redirect     off;

            # pass websocket upgrade headers
            proxy_http_version 1.1;
            proxy_set_header   Upgrade $http_upgrade;
            proxy_set_header   Connection $connection_upgrade;

            # for spring security -- we don't seem to need this though.
            # proxy_pass_header X-XSRF-TOKEN;

            # long timeout to allow for WS connections idle for longer than the default 60s
            proxy_read_timeout 6000s;

            proxy_intercept_errors on;
            proxy_cache_bypass $http_upgrade;

            access_log  /var/log/nginx/access.log  upstream_ws;

        }

        # For proxying websockets
        location = /vmturbo/remoteMediation {
            set $topology_processor_servers $TOPOLOGY;

            # For proxying of websockets to topology processor SSL port
            #proxy_pass https://$topology_processor_servers:9443/remoteMediation;
            # For proxying to non-SSL topology processor
            proxy_pass http://$topology_processor_servers:8080/remoteMediation;

            proxy_set_header   Host $http_host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header   X-Forwarded-Host $server_name;

            proxy_set_header   X-Forwarded-Proto $scheme;
            proxy_set_header   X-NginX-Proxy true;
            proxy_redirect     off;

            # pass websocket upgrade headers
            proxy_http_version 1.1;
            proxy_set_header   Upgrade $http_upgrade;
            proxy_set_header   Connection $connection_upgrade;

            # for spring security -- we don't seem to need this though.
            # proxy_pass_header X-XSRF-TOKEN;

            # long timeout to allow for WS connections idle for longer than the default 60s
            proxy_read_timeout 6000s;

            proxy_intercept_errors on;
            proxy_cache_bypass $http_upgrade;

            access_log  /var/log/nginx/access.log  upstream_ws;

        }

        # the maintenance page -- show "server temporarily unavailable" page
        location ^~ /maintenance/ {
            root /var/www;
            index monitor.html;
        }

        # /status will show the load_status contents, if they exist -- static text o/w
        location /status {
            root /var/www;
            try_files /status/load_status /maintenance/default_status;
        }

        # Diags download can take a long time. So we set the read timeout to a
        # higher value.
        location = /api/v3/cluster/diagnostics {
            set $api_servers $API;
            proxy_pass         http://$api_servers:8080;
            proxy_set_header   Host $http_host;
            proxy_set_header   X-Real-IP $remote_addr;
            proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 10s;
            proxy_read_timeout 7200s; # 2 hours
            proxy_intercept_errors on;

            # Ensure the proxy-side redirect from API gets rewritten to the gateway address
            proxy_set_header   X-Forwarded-Proto $scheme;
            proxy_redirect     http://$http_host/ $scheme://$http_host/;

            # Send 503 "service unavailable" on gateway issues, where API may be down or unavailable
            error_page 502 503 504 =503 /maintenance/;

            access_log  /var/log/nginx/access.log  upstream_ws;
        }

        # include any additional server config files for the secured block
        include /etc/nginx/userconf/secure/*.conf;

    }

    # include any additional config files for the main http block
    include /etc/nginx/userconf/http/*.conf;
}

EOF

# doing cat and re-direct to preserve permissions of the destination file.
cat $NGINX_CONF_TMPL_SRC > $NGINX_CONF_TMPL_DEST
