#!/bin/bash

while [ "${1-}" != "" ]; do
  case $1 in
  -n)
    shift
    namespace="-n ${1}"
    ;;
  *)
    echo "Invalid option: ${1}" >&2
    exit 1
    ;;
  esac
  shift
done

kubectl patch tc turbonomicclient-release ${namespace} --type merge -p $'''
spec:
  global:
    registry: icr.io/cpopen
  tunnel:
    siteController:
      image:
        name: turbonomic/skupper-site-controller
    serviceController:
      image:
        name: turbonomic/skupper-service-controller
    router:
      image:
        name: turbonomic/skupper-router
    configSync:
      image:
        name: turbonomic/skupper-config-sync
  probes:
    actionScript:
      image:
        name: turbonomic/mediation-actionscript
    actionStreamKafka:
      image:
        name: turbonomic/mediation-actionstream-kafka
    appDynamics:
      image:
        name: turbonomic/mediation-appdynamics
    appInsights:
      image:
        name: turbonomic/mediation-appinsights
    bareMetal:
      image:
        name: turbonomic/mediation-baremetal
    compellent:
      image:
        name: turbonomic/mediation-compellent
    datadog:
      image:
        name: turbonomic/mediation-datadog
    dynatrace:
      image:
        name: turbonomic/mediation-dynatrace
    flexera:
      image:
        name: turbonomic/mediation-flexera
    hds:
      image:
        name: turbonomic/mediation-hds
    horizon:
      image:
        name: turbonomic/mediation-horizon
    hpe3par:
      image:
        name: turbonomic/mediation-hpe3par
    hyperFlex:
      image:
        name: turbonomic/mediation-hyperflex
    hyperV:
      image:
        name: turbonomic/mediation-hyperv
    powerVM:
      image:
        name: turbonomic/mediation-powervm
    ibmStorageFlashSystem:
      image:
        name: turbonomic/mediation-ibmstorage-flashsystem
    instana:
      image:
        name: turbonomic/mediation-instana
    jBoss:
      image:
        name: turbonomic/mediation-jboss
    jvm:
      image:
        name: turbonomic/mediation-jvm
    mssql:
      image:
        name: turbonomic/mediation-mssql
    mysql:
      image:
        name: turbonomic/mediation-mysql
    netApp:
      image:
        name: turbonomic/mediation-netapp
    newRelic:
      image:
        name: turbonomic/mediation-newrelic
    nutanix:
      image:
        name: turbonomic/mediation-nutanix
    oneView:
      image:
        name: turbonomic/mediation-oneview
    oracle:
      image:
        name: turbonomic/mediation-oracle
    pure:
      image:
        name: turbonomic/mediation-pure
    scaleIO:
      image:
        name: turbonomic/mediation-scaleio
    serviceNow:
      image:
        name: turbonomic/mediation-servicenow
    snmp:
      image:
        name: turbonomic/mediation-snmp
    tanium:
      image:
        name: turbonomic/mediation-tanium
    terraform:
      image:
        name: turbonomic/mediation-terraform
    tomcat:
      image:
        name: turbonomic/mediation-tomcat
    ucs:
      image:
        name: turbonomic/mediation-ucs
    ucsDirector:
      image:
        name: turbonomic/mediation-ucsdirector
    vCenter:
      mediationVCenter:
        image:
          name: turbonomic/mediation-vcenter
      mediationVCenterBrowsing:
        image:
          name: turbonomic/mediation-vcenterbrowsing
    vmax:
      image:
        name: turbonomic/mediation-vmax
    vmm:
      image:
        name: turbonomic/mediation-vmm
    vplex:
      image:
        name: turbonomic/mediation-vplex
    webLogic:
      image:
        name: turbonomic/mediation-weblogic
    wmi:
      image:
        name: turbonomic/mediation-wmi
    xen:
      image:
        name: turbonomic/mediation-xen
    xtremIO:
      image:
        name: turbonomic/mediation-xtremio
'''
