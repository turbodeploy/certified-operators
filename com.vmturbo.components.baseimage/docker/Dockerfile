FROM registry.access.redhat.com/ubi8
MAINTAINER Billy O'Connell <billy.oconnell@turbonomic.com>

# Install rsyslog and OpenJDK java-11 packages
RUN dnf install -y rsyslog procps-ng python2 java-11-openjdk-headless net-tools xz gnutls && \
    dnf clean all && rm -rf /var/cache/dnf

# Install OpenJ9
RUN curl -k -s -O -L https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.6%2B10_openj9-0.18.0/OpenJDK11U-jre_x64_linux_openj9_11.0.6_10_openj9-0.18.0.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xf OpenJDK11U-jre_x64_linux_openj9_11.0.6_10_openj9-0.18.0.tar.gz --directory /usr/lib/jvm/ && \
    rm OpenJDK*.tar.gz
# set the path to the OpenJ9 JVM in case we want to use it.
ENV J9PATH="/usr/lib/jvm/jdk-11.0.6+10-jre/bin:${PATH}"

COPY entrypoint.sh collect_diags.sh diags.py dump_histogram.sh wait_for_full_gc.sh terminate.sh /
RUN chmod 755 /entrypoint.sh && chmod 755 /collect_diags.sh && chmod 755 /terminate.sh && \
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
