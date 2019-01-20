FROM ubuntu:18.04
MAINTAINER Michael Borodiansky<michael.borodiansky@turbonomic.com>

RUN apt-get update -q -y && apt-get upgrade -q -y && \
apt-get install -q -y ca-certificates python2.7 zip xz-utils rsyslog bash && \
addgroup --gid 1000 vmtsyslog && adduser --uid 1000 --system --disabled-password -home /home/vmtsyslog --ingroup vmtsyslog --disabled-login --shell /bin/bash vmtsyslog && \
mkdir -p /home/vmtsyslog/rsyslog && \
chown -R vmtsyslog:vmtsyslog /home/vmtsyslog && \
mkdir -p /var/lib/logrotate && \
chmod a+w /var/lib/logrotate && \
mkdir -p /var/log/turbonomic && \
chown -R vmtsyslog:vmtsyslog /var/log/turbonomic

COPY entrypoint.sh /
ADD rsyslog.conf /etc
COPY diags.py /
COPY collect_diags.sh /
COPY collect_diags_proactive.sh /
COPY logrotate.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /diags.py && chmod 755 /collect_diags.sh && chmod 755 /logrotate.sh && chmod 755 /collect_diags_proactive.sh

EXPOSE 2514

USER vmtsyslog

VOLUME ["/home/vmtsyslog", "/var/lib/logrotate", "/var/log/turbonomic"]
ENTRYPOINT ["/entrypoint.sh"]

