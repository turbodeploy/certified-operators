FROM alpine
MAINTAINER Michael Borodiansky<michael.borodiansky@turbonomic.com>

RUN addgroup -g 1000 vmtsyslog && adduser -S -D -h /home/vmtsyslog -s /bin/bash -G vmtsyslog -u 1000 vmtsyslog && passwd -l root

RUN apk update && \
apk add --no-cache ca-certificates python2 zip xz rsyslog bash && \
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

