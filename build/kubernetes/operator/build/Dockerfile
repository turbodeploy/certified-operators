FROM quay.io/operator-framework/helm-operator:v0.7.0

# Required OpenShift Labels
LABEL name="Turbonomic Operator" \
      vendor="Turbonomic" \
      version="v7.18.0" \
      release="1" \
      summary="This is the t8c operator." \
      description="This operator will deploy an instance of t8c with persistent storage."

# Required Licenses
COPY licenses /licenses

COPY watches.yaml ${HOME}/watches.yaml
COPY helm-charts/ ${HOME}/helm-charts/
