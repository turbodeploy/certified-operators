FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="nginx" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="nginx" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

ENV NAME=nginx \
    NGINX_VERSION=1.20 \
    NGINX_SHORT_VER=120 \
    VERSION=0

# Required Licenses
COPY licenses /licenses

# Add user and group beforehand, so that rpm won't create its own with differen uid/gid.
RUN groupadd -g 1000 nginx && useradd -r -g 1000 -s /bin/bash -u 102 nginx && \
passwd -l root

# Install rsyslog and nginx packages
RUN dnf -y module enable nginx:$NGINX_VERSION && \
    INSTALL_PKGS="rsyslog procps-ng nss_wrapper bind-utils gettext hostname nginx nginx-mod-stream nginx-mod-http-perl" && \
    dnf install -y --setopt=tsflags=nodocs $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    dnf update -y krb5-libs \
    dnf -y clean all --enablerepo='*' &&\
    rm -rf /var/cache/dnf

# Create a user and group first so the IDs get set the same way, even as
# the rest of this may change over time.
RUN mkdir -p /var/log/nginx/logs && \
    chown -R nginx:nginx /var/log/nginx/logs && \
    mkdir -p /var/cache/nginx && \
    chown -R nginx:nginx /var/cache/nginx && \
    touch /var/run/nginx.pid && \
    chown -R nginx:nginx /var/run/nginx.pid && \
    mkdir -p /etc/nginx/certs && \
    chown -R nginx:nginx /etc/nginx/certs

# rsyslog
ADD rsyslog.conf /etc

COPY nginx.conf.template /etc/nginx/
COPY includes /etc/nginx/includes
RUN chown -R nginx:nginx /etc/nginx/includes
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

