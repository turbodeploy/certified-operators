FROM ubuntu:17.04
MAINTAINER Michael Borodiansky <michael.borodiansky@turbonomic.com>

RUN apt-get update -q -y && apt-get upgrade -q -y && \
apt-get install -q -y --no-install-recommends rsyslog python2.7 xz-utils net-tools vim-tiny openjdk-8-jdk && \
rm -rf /var/lib/apt/lists/* && apt-get clean

COPY entrypoint.sh /
COPY collect_diags.sh /
COPY diags.py /
COPY logrotate.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /collect_diags.sh && \
chmod 755 /diags.py && chmod 755 /logrotate.sh

# rsyslog
ADD rsyslog.conf /etc

# Create a user which is not allowed to logon in the system.
RUN adduser --system --disabled-password -home /home/turbonomic --ingroup staff --disabled-login --shell /bin/bash turbonomic && \
passwd -l root

USER turbonomic

RUN mkdir -p /home/turbonomic/data

VOLUME ["/home/turbonomic/data"]

ENTRYPOINT ["/entrypoint.sh"]

