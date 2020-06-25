#!/bin/bash

XL_RELEASE=$(kubectl get xls.charts.helm.k8s.io | awk '{print $1}' | egrep -v NAME)

echo "----------"
echo pvc
for i in $(kubectl get pvc | awk '{print $1}' | egrep -v NAME)
do
  echo $i
  KIND=pvc
  NAME=${i}
  RELEASE=${XL_RELEASE}
  NAMESPACE=turbonomic
  kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-name=${RELEASE} &>/dev/null
  kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-namespace=${NAMESPACE} &>/dev/null
done

echo "----------"
echo svc
for i in $(kubectl get svc | awk '{print $1}' | egrep -v NAME | egrep -v "glusterfs-dynamic")
do
  echo $i
  KIND=svc
  NAME=${i}
  RELEASE=${XL_RELEASE}
  NAMESPACE=turbonomic
  kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-name=${RELEASE} &>/dev/null
  kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-namespace=${NAMESPACE} &>/dev/null
done

echo "----------"
echo deployment
for i in $(kubectl get deployment | awk '{print $1}' | egrep -v NAME)
do
  echo $i
  KIND=deployment
  NAME=${i}
  RELEASE=${XL_RELEASE}
  NAMESPACE=turbonomic
  kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-name=${RELEASE} &>/dev/null
  kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-namespace=${NAMESPACE} &>/dev/null
done

echo "----------"
echo endpoint
echo db
KIND=ep
NAME=db
RELEASE=${XL_RELEASE}
NAMESPACE=turbonomic
kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-name=${RELEASE} &>/dev/null
kubectl annotate ${KIND} ${NAME} meta.helm.sh/release-namespace=${NAMESPACE} &>/dev/null
kubectl label ${KIND} ${NAME} app.kubernetes.io/managed-by=Helm &>/dev/null
