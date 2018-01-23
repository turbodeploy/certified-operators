#!/bin/bash

# Functions used throughout the script
usage()
{
  echo "Use: `basename $0` DOCKER_ISO"
  echo "File should be located in ~/tmp/"
  exit -1
}

DOCKER_ISO=${1}

[ -z "${DOCKER_ISO}" ] && usage

# Test if file exists
if [ -f ~/tmp/$DOCKER_ISO ]; then
  rm -rf turbonomic_xl/ ova/
  sed "s#XL_ISO#${HOME}/tmp/${DOCKER_ISO}#g" centos7_vmw.json > /tmp/centos7_vmw.json.edited
  time packer build /tmp/centos7_vmw.json.edited
  mkdir ova
  ovftool turbonomic_xl/turbonomic_xl.vmx  ova/turbonomic_xl.ova
  rm /tmp/centos7_vmw.json.edited
else
  echo "The file '~/tmp/$DOCKER_ISO' in not found."
  echo "Please specify the proper iso."
fi