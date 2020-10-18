FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Required OpenShift Labels
LABEL name="diags" \
      vendor="Turbonomic" \
      version="7" \
      release="1" \
      summary="diags" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Required Licenses
COPY licenses /licenses

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng python2 net-tools java-11-openjdk-devel xz \
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
# Install OpenJ9
# https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10_openj9-0.21.0/OpenJDK11U-jdk_x64_linux_openj9_11.0.8_10_openj9-0.21.0.tar.gz
RUN curl -k -s -O -L https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10_openj9-0.21.0/OpenJDK11U-jdk_x64_linux_openj9_11.0.8_10_openj9-0.21.0.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xf OpenJDK11U-jdk_x64_linux_openj9_11.0.8_10_openj9-0.21.0.tar.gz --directory /usr/lib/jvm/ && \
    rm OpenJDK*.tar.gz
# set a path variable if we want to use J9 instead of HotSpot
ENV J9PATH="/usr/lib/jvm/jdk-11.0.8+10/bin:${PATH}"

COPY entrypoint.sh collect_diags.sh diags.py dump_histogram.sh wait_for_full_gc.sh terminate.sh run.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /collect_diags.sh && chmod 755 /terminate.sh && \
    chmod 755 /diags.py && chmod 755 /dump_histogram.sh && chmod 755 /wait_for_full_gc.sh && chmod 755 /run.sh

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
