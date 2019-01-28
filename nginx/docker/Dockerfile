FROM alpine
MAINTAINER Patrick Soo Hoo <patrick.soohoo@turbonomic.com>

# Install nginx and other tools
RUN apk update && apk add --no-cache bash net-tools rsyslog openssl nginx util-linux gettext

# Create a user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
RUN passwd -l root && \
    mkdir -p /var/log/nginx/logs && \
    chown -R nginx:nginx /var/log/nginx/logs && \
    mkdir -p /var/tmp/nginx && \
    chown -R nginx:nginx /var/tmp/nginx && \
    touch /var/run/nginx.pid && \
    chown -R nginx:nginx /var/run/nginx.pid && \
    mkdir -p /etc/nginx/certs && \
    chown -R nginx:nginx /etc/nginx/certs && \
    chown -R nginx:nginx /var/lib/nginx

# rsyslog
ADD rsyslog.conf /etc

COPY nginx.conf.template /home/nginx/conf/
COPY entrypoint.sh /

# copy static html resources
COPY www /var/www/

# forward request and error logs to docker log collector
RUN ln -sf /dev/stdout /var/log/nginx/access.log && \
	ln -sf /dev/stderr /var/log/nginx/error.log && \
    ln -sf /dev/stdout /var/log/nginx/logs/access.log && \
	ln -sf /dev/stderr /var/log/nginx/logs/error.log && \
	chmod 755 /entrypoint.sh

EXPOSE 8000
EXPOSE 8443

USER nginx

VOLUME /etc/nginx/certs
VOLUME /etc/nginx/userconf

STOPSIGNAL SIGTERM

ENTRYPOINT ["/entrypoint.sh"]

