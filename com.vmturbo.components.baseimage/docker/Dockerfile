FROM ubuntu:18.04
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

RUN apt-get update -q -y && apt-get upgrade -q -y && \
apt-get install -q -y --no-install-recommends rsyslog python2.7 openjdk-8-jre-headless ca-certificates-java && \
rm -rf /var/lib/apt/lists/* && apt-get clean

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
RUN adduser --system --disabled-password -home /home/turbonomic --ingroup staff --disabled-login --shell /bin/bash turbonomic && \
passwd -l root

USER turbonomic

ENTRYPOINT ["/entrypoint.sh"]

