FROM alpine
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

RUN apk update && \
apk add --no-cache rsyslog python2 openjdk8-jre-base ca-certificates bash util-linux

COPY entrypoint.sh /
COPY collect_diags.sh /
COPY diags.py /
COPY dump_histogram.sh /
COPY wait_for_full_gc.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /collect_diags.sh && \
chmod 755 /diags.py && chmod 755 /dump_histogram.sh && chmod 755 /wait_for_full_gc.sh

# add the swagger UI
COPY swagger /swagger/

# rsyslog
ADD rsyslog.conf /etc

# Create a user which is not allowed to logon in the system.
RUN addgroup staff && adduser -S -D -h /home/turbonomic -s /bin/bash -G staff turbonomic && passwd -l root

USER turbonomic

RUN mkdir -p /home/turbonomic/data

ENTRYPOINT ["/entrypoint.sh"]

