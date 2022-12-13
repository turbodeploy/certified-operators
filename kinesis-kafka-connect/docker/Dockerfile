ARG kafka_connect_version
ARG connector_version

FROM registry.access.redhat.com/ubi8 AS build
MAINTAINER "Billy O'Connell <billy.oconnell@turbonomic.com>"
# Create a user and lock the root user's password
RUN groupadd -g 1000 kinesis && useradd -r -g 1000 -s /bin/bash -u 102 kinesis && \
    passwd -l root

# Install maven
RUN dnf install maven unzip -y

# Create a home directory so that maven can create its ~/.m2 directory
RUN mkdir /home/kinesis
RUN chown -R kinesis:kinesis /home/kinesis

# The connector's source will be unzipped and compiled in the /connector directory
WORKDIR /connector
RUN chown -R kinesis:kinesis /connector
USER kinesis

# Download the source code for the connector plugin. The connector_version variable is set at the global scope
ARG connector_version
RUN curl -L "https://github.com/awslabs/kinesis-kafka-connector/archive/${connector_version}.zip" \
         -o connector.zip

# Unzip the code. We expect to get a directory called kinesis-kafka-connector-0.0.8 (if we use version 0.0.8)
RUN unzip connector.zip

# Build the connector plugin (jar)
RUN mvn -f kinesis-kafka-connector-$connector_version package

FROM confluentinc/cp-kafka-connect:${kafka_connect_version}
MAINTAINER "Billy O'Connell <billy.oconnell@turbonomic.com>"

# Required OpenShift Labels
LABEL name="kinesis-kafka-connect" \
      vendor="Turbonomic" \
      version="8" \
      release="0" \
      summary="kinesis-kafka-connect" \
      description="Turbonomic Workload Automation for Multicloud simultaneously optimizes performance, compliance, and cost in real-time. Workloads are precisely resourced, automatically, to perform while satisfying business constraints."

# Copy Turbo license file and Apache 2 license file
COPY licenses /licenses

# Add the plugin version to the environment for debugging
ARG connector_version
ENV CONNECTOR_VERSION=$connector_version

# Install common libs that every component has
USER root
RUN curl https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -s -o /etc/yum.repos.d/epel-release-latest-8.noarch.rpm && \
    dnf install -y --nobest rsyslog procps-ng && \
    # clean up
    dnf clean all

# Copy over the rssyslog configuration
ADD rsyslog.conf /etc

# appuser needs permission to write certificate and setup configs to tmp.
RUN chown appuser:appuser /tmp

# Switch to the user provided by the base image
USER appuser

# Add the entrypoint script
WORKDIR /home/appuser
COPY --chown=appuser:appuser entrypoint.sh ./
RUN chmod u+x entrypoint.sh

# Get the kinesis plugin jar and place it where in the kinesis-kafka-connect-plugin directory.
# Kafka connect will look for plugins in this directory because of the plugin.path property
# in worker.properties.
RUN mkdir kinesis-kafka-connect-plugin
COPY --from=build --chown=appuser:appuser /connector/kinesis-kafka-connector-*/target/amazon-kinesis-*.jar \
                  kinesis-kafka-connect-plugin/

# Get the properties template files. Some properties use a ${} assignment. This will be replaces
# with external configuration by the entrypoint script. The values to be replaced are expected to
# show up as environment variables.
COPY --chown=appuser:appuser worker.properties kinesis-streams-kafka-connect.properties ./

ENTRYPOINT ["./entrypoint.sh"]


