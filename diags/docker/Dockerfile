FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng python2 java-11-openjdk-headless \
        openssl gettext iputils iproute nmap lsof openssh-clients && \
    echo "[fedora]" > /etc/yum.repos.d/fedora.repo && \
    echo "baseurl = https://download.fedoraproject.org/pub/fedora/linux/releases/30/Everything/x86_64/os" >> /etc/yum.repos.d/fedora.repo && \
    echo "gpgcheck=0" >> /etc/yum.repos.d/fedora.repo && \
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
