FROM ubuntu:17.10
MAINTAINER Patrick Soo Hoo <patrick.soohoo@turbonomic.com>

SHELL ["/bin/bash", "-c"]

# Install nginx and other tools
RUN apt-get -q -y update && apt-get -q -y --no-install-recommends upgrade && \
    apt-get install -q -y --no-install-recommends bash net-tools vim-tiny rsyslog openssl nginx gettext-base && \
    rm -rf /var/lib/apt/lists/* && apt-get clean

# Create a user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
RUN addgroup nginx && \
    adduser --system --disabled-password -home /var/cache/nginx --ingroup nginx --disabled-login --shell /bin/bash nginx && \
    touch /var/run/nginx.pid && \
    chown -R nginx:nginx /var/run/nginx.pid && \
    mkdir -p /etc/nginx/certs && \
    chown -R nginx:nginx /etc/nginx/certs && \
    mkdir -p /var/log/nginx && \
    mkdir -p /home/nginx/conf && \
    chown -R nginx:nginx /var/log/nginx && \
    chown -R nginx:nginx /var/lib/nginx

# rsyslog
ADD rsyslog.conf /etc

COPY nginx.conf.template /home/nginx/conf/
RUN chown -R nginx:nginx /home/nginx
COPY entrypoint.sh /

# copy static html resources
COPY www /var/www/

# forward request and error logs to docker log collector
RUN ln -sf /dev/stdout /var/log/nginx/access.log && \
	ln -sf /dev/stderr /var/log/nginx/error.log && \
	chmod 755 /entrypoint.sh

EXPOSE 8000
EXPOSE 8443

USER nginx

VOLUME /etc/nginx/certs
VOLUME /etc/nginx/userconf

STOPSIGNAL SIGTERM

ENTRYPOINT ["/entrypoint.sh"]

