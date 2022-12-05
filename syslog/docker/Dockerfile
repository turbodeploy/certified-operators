FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="syslog" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="syslog" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

RUN groupadd -g 1000 vmtsyslog && useradd -m -r -d /home/vmtsyslog -g 1000 -s /bin/bash -u 1000 vmtsyslog && \
    passwd -l root

# Install rsyslog package
RUN dnf install -y rsyslog procps-ng zip xz python2 gnutls wget  && \
    dnf update -y krb5-libs \
    dnf clean all && \
    rm -rf /var/cache/dnf && \
    mkdir -p /home/vmtsyslog/rsyslog && \
    chown -R vmtsyslog:vmtsyslog /home/vmtsyslog && \
    mkdir -p /var/lib/logrotate && \
    chmod a+w /var/lib/logrotate && \
    mkdir -p /var/log/turbonomic && \
    chown -R vmtsyslog:vmtsyslog /var/log/turbonomic

# Install yq for yaml properties extraction.
ARG YQ_VERSION=v4.27.3
ARG YQ_BINARY=yq_linux_386
RUN wget https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/${YQ_BINARY} -O /usr/bin/yq && \
    chmod +x /usr/bin/yq

COPY entrypoint.sh /
ADD rsyslog.conf /etc
COPY diags.py /
COPY collect_diags.sh /
COPY collect_diags_proactive.sh /
COPY logrotate.sh /
COPY logrotate_product.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /diags.py && chmod 755 /collect_diags.sh && chmod 755 /logrotate_product.sh && chmod 755 /logrotate.sh && chmod 755 /collect_diags_proactive.sh

EXPOSE 2514

USER vmtsyslog

VOLUME ["/home/vmtsyslog", "/var/lib/logrotate", "/var/log/turbonomic"]
ENTRYPOINT ["/entrypoint.sh"]

