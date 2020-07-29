#!/bin/bash

# Need to check if the helm release is uid based (xl-release-XXX instead of xl-release)
# and if it is, can convert it before or after the upgrade of the operator
helmRelease=$(kubectl get xls -o yaml|grep name:|head -1 | awk '{print $2}')
secretName=$(kubectl get pvc -o yaml -n turbonomic | grep "app.kubernetes.io/instance" | uniq | awk '{print $2}')
# Exit if the uid based release has already been converted
if [ "$helmRelease" = "$secretName" ]; then
  echo "helm release has already been converted. Exiting."
  exit 0
fi

echo "Scale down the Operator"
echo "-----------------------"
kubectl scale deployment --replicas=0 t8c-operator -n turbonomic
operatorCount=$(kubectl get pod | grep t8c-operator | wc -l)
while [ ${operatorCount} -gt 0 ]
do
 operatorCount=$(kubectl get pod | grep t8c-operator | wc -l)
done
echo

# Current instance annotation
helmRelease=$(kubectl get pvc -o yaml -n turbonomic | grep "app.kubernetes.io/instance" | uniq | awk '{print $2}' | cut -d- -f1,2)

# Update pvcs
cat << EOF > /tmp/pvc-patch.yml
metadata:
  labels:
    app.kubernetes.io/instance: ${helmRelease}
EOF

echo "----------"
echo "pvc update annotations"
for pvc in $(kubectl get pvc -n turbonomic | awk '{print $1}' | egrep -v NAME)
do
  kubectl patch pvc ${pvc} -n turbonomic --type merge --patch "$(cat /tmp/pvc-patch.yml)"
done
echo


# Update deployments
cat << EOF > /tmp/deployment-patch.yml
metadata:
  labels:
    app.kubernetes.io/instance: ${helmRelease}
spec:
  selector:
    matchLabels:
      app.kubernetes.io/instance: ${helmRelease}
  template:
    metadata:
      labels:
        app.kubernetes.io/instance: ${helmRelease}
EOF

echo "----------"
echo "deployment updates"
for deployment in $(kubectl get deployment -n turbonomic | awk '{print $1}' | egrep -v NAME)
do
  kubectl patch deployment ${deployment} -n turbonomic --type merge --patch "$(cat /tmp/deployment-patch.yml)"
done
echo

# Update ConfigMap
cat << EOF > /tmp/cm-patch.yml
  metadata:
    labels:
      app.kubernetes.io/instance: ${helmRelease}
EOF

configMap=$(kubectl get cm -n turbonomic | grep global | awk '{print $1}')

echo "----------"
echo "configmap updates"
kubectl patch cm ${configMap} -n turbonomic --type merge --patch "$(cat /tmp/cm-patch.yml)"

## Update secrets
echo "----------"
echo "secrets updates"
kubectl get secrets -n turbonomic $(kubectl get secrets -n turbonomic | grep ${helmRelease} | awk '{print $1}' | head -1) -o yaml > /tmp/helm-release.yml
sed -i "s/${helmRelease}-[0-9A-Za-z]*/${helmRelease}/g" /tmp/helm-release.yml
kubectl get secrets -n turbonomic $(kubectl get secrets -n turbonomic | grep ${helmRelease} | awk '{print $1}' | head -1) -o jsonpath={.data.release} | base64 -d | base64 -d |gunzip > /tmp/xl-release
sed -i "s/${helmRelease}-[0-9A-Za-z]*/${helmRelease}/g" /tmp/helm-release.yml
gzip -c /tmp/xl-release | base64 -w 0 | base64 -w 0 > /tmp/xl-updated-release
data=$(cat /tmp/xl-updated-release)
sed -i "s/release:.*/release: ${data}/g" /tmp/helm-release.yml

# Delete the latest secret and apply the updated one
for secrets in $(kubectl get secrets -n turbonomic | grep ${helmRelease} | awk '{print $1}')
do
  kubectl delete secrets -n turbonomic ${secrets}
done

# Apply the new release
kubectl apply -f /tmp/helm-release.yml -n turbonomic
echo

# Scale the operator back up
echo "Scale up the Operator"
echo "---------------------"
kubectl scale deployment --replicas=1 t8c-operator -n turbonomic
