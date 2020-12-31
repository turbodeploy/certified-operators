#!/bin/bash

# Allow kubernetes to do an offline update with no internet access.

source /opt/local/etc/turbo.conf

sudo /usr/local/bin/kubeadm reset -f > /dev/null 2>&1
sudo systemctl stop etcd > /dev/null 2>&1
sudo rm -rf /var/lib/etcd/member > /dev/null 2>&1
sudo rm -rf /etc/ssl/etcd/ssl > /dev/null 2>&1
sudo rm -rf /etc/kubernetes/ssl > /dev/null 2>&1
pushd /etc/; for i in `sudo grep -lr 10.0.2.15 *`; do sudo sed -i "s/10.0.2.15/${node}/g" $i; done; popd
sudo rm -rf /root/.kube > /dev/null 2>&1
sudo rm -rf /opt/turbonomic/.kube > /dev/null 2>&1

