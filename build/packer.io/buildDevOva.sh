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
  sed -i.bak "s#XL_ISO#${HOME}/tmp/${DOCKER_ISO}#g" centos7_vmw.json
  packer build centos7_vmw.json
  mkdir ova
  ovftool turbonomic_xl/turbonomic_xl.vmx  ova/turbonomic_xl.ova
  cp centos7_vmw.json.bak centos7_vmw.json
else
  echo "The file '~/tmp/$DOCKER_ISO' in not found."
  echo "Please specify the proper iso."
fi