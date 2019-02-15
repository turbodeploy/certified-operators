FROM ubuntu:18.04
MAINTAINER Michael Borodiansky <michael.borodiansky@turbonomic.com>
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y iputils-ping iproute2 nmap tcpdump lsof telnet \
sysstat strace iptraf vim mc openssh-client openssl iftop traceroute whowatch && \
mkdir /root/.ssh

ADD readme.txt /README.txt
COPY ssh_config /root/.ssh/config

ENTRYPOINT ["/bin/bash"]

