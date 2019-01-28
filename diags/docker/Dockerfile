FROM alpine
MAINTAINER Michael Borodiansky <michael.borodiansky@turbonomic.com>
RUN apk update && apk add --no-cache iputils iproute2 nmap tcpdump lsof \
sysstat strace vim openssh-client openssl iftop tcptraceroute && \
mkdir /root/.ssh

ADD readme.txt /README.txt
COPY ssh_config /root/.ssh/config

ENTRYPOINT ["/bin/bash"]

