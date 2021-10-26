FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="diags" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="diags" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

# This is the release of openj9 to pull in.
ENV OPENJ9_URL=https://github.com/ibmruntimes/semeru11-binaries/releases/download/jdk-11.0.13%2B8_openj9-0.29.0/ibm-semeru-open-11-jdk-11.0.13.8_0.29.0-1.x86_64.rpm

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng python2 net-tools xz \
        openssl gettext iputils iproute nmap lsof openssh-clients && \
    echo "[CentOS_Base]" > /etc/yum.repos.d/base.repo && \
    echo "baseurl = http://mirror.centos.org/centos/8/BaseOS/x86_64/os/" >> /etc/yum.repos.d/base.repo && \
    echo "gpgcheck=0" >> /etc/yum.repos.d/base.repo && \
    echo "[CentOS_AppStream]" > /etc/yum.repos.d/appstream.repo && \
    echo "baseurl = http://mirror.centos.org/centos/8/AppStream/x86_64/os/" >> /etc/yum.repos.d/appstream.repo && \
    echo "gpgcheck=0" >> /etc/yum.repos.d/appstream.repo && \
    dnf install -y tcpdump telnet sysstat strace iptraf-ng traceroute && \
    dnf install -y $OPENJ9_URL && \
    dnf clean all && \
    rm -rf /var/cache/dnf && \
    mkdir /root/.ssh

COPY entrypoint.sh collect_diags.sh diags.py terminate.sh run.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /collect_diags.sh && chmod 755 /terminate.sh && \
    chmod 755 /diags.py && chmod 755 /run.sh

# add the swagger UI
COPY swagger /swagger/

# rsyslog
ADD rsyslog.conf /etc

ADD readme.txt /README.txt
COPY ssh_config /root/.ssh/config

# add this so future interactive sessions will have the appropriate jvm path set.
COPY jvmpath.sh /etc/profile.d/jvmpath.sh

# Create a user which is not allowed to logon in the system.
RUN useradd -m -r -d /home/turbonomic -g 50 -s /bin/bash -u 102 turbonomic && \
    passwd -l root && echo "source /etc/bashrc" >> /root/.bashrc

RUN chmod 755 /home/turbonomic && mkdir -p /home/turbonomic/data

ENTRYPOINT ["/entrypoint.sh"]
