FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Install rsyslog and java-11 packages
RUN dnf install -y rsyslog procps-ng python2 net-tools xz && \
    dnf clean all && \
    rm -rf /var/cache/dnf && \
    curl -k -s -O -L https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10_openj9-0.18.0/OpenJDK11U-jre_x64_linux_openj9_11.0.6_10_openj9-0.18.0.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xf OpenJDK11U-jre_x64_linux_openj9_11.0.6_10_openj9-0.18.0.tar.gz --directory /usr/lib/jvm/ && \
    rm OpenJDK*.tar.gz
ENV PATH="/usr/lib/jvm/jdk-11.0.6+10-jre/bin:${PATH}"

COPY entrypoint.sh collect_diags.sh diags.py dump_histogram.sh wait_for_full_gc.sh /
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
