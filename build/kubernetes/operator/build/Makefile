version:=8.0
# Note that this makefile is only used to ensure helm dependency updates are done in the correct
# order. It always rebuilds everything.
# This is a temporary workaround until https://github.com/helm/helm/issues/2247 is implemented.
all: operator

operator: xl
	cd .. ; operator-sdk build "turbonomic/t8c-operator:$(version)"

xl: prometheus
	helm dependency update "../helm-charts/xl"

prometheus:
	helm dependency update "../helm-charts/base/prometheus"

.PHONY: all operator helm prometheus
