FROM registry.access.redhat.com/rhel7
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

RUN yum install -y rsyslog java-11-openjdk-headless && \
    yum clean all && \
    rm -rf /var/cache/yum

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
RUN useradd -m -r -d /home/turbonomic -g 50 -s /bin/bash -u 102 turbonomic && \
    passwd -l root

USER turbonomic

RUN mkdir -p /home/turbonomic/data

ENTRYPOINT ["/entrypoint.sh"]
