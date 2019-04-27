FROM registry.access.redhat.com/rhel7
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

RUN yum install -y rsyslog java-1.8.0-openjdk-devel \
    openssl rsyslog gettext iputils iproute nmap tcpdump lsof \
    telnet sysstat strace iptraf-ng openssh-clients traceroute && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    mkdir /root/.ssh

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

ADD readme.txt /README.txt
COPY ssh_config /root/.ssh/config

RUN mkdir -p /home/turbonomic/data

ENTRYPOINT ["/entrypoint.sh"]
