FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng python2 java-11-openjdk-devel net-tools xz \
        openssl gettext iputils iproute nmap lsof openssh-clients && \
    echo "[CentOS_Base]" > /etc/yum.repos.d/base.repo && \
    echo "baseurl = http://mirror.centos.org/centos/8/BaseOS/x86_64/os/" >> /etc/yum.repos.d/base.repo && \
    echo "gpgcheck=0" >> /etc/yum.repos.d/base.repo && \
    echo "[CentOS_AppStream]" > /etc/yum.repos.d/appstream.repo && \
    echo "baseurl = http://mirror.centos.org/centos/8/AppStream/x86_64/os/" >> /etc/yum.repos.d/appstream.repo && \
    echo "gpgcheck=0" >> /etc/yum.repos.d/appstream.repo && \
    dnf install -y tcpdump telnet sysstat strace iptraf-ng traceroute && \
    dnf clean all && \
    rm -rf /var/cache/dnf && \
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
