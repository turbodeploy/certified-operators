
IMG_PREFIX ?= turbonomic
IMG_TAG ?= latest
ADD_ARGS ?=

docker-build: docker-build-site-controller docker-build-service-controller docker-build-config-sync docker-build-router

SITE_CONTROLLER_IMAGE ?= quay.io/skupper/site-controller
SITE_CONTROLLER_VERSION ?= 1.0.1
docker-build-site-controller:
	docker build $(ADD_ARGS) -t $(IMG_PREFIX)/skupper-site-controller:$(IMG_TAG) -f docker/Dockerfile \
	--build-arg BASE_IMAGE=$(SITE_CONTROLLER_IMAGE):$(SITE_CONTROLLER_VERSION) \
	--build-arg COMMAND=/app/site-controller \
	--build-arg COMPONENT_NAME=skupper-site-controller \
	./docker

SERVICE_CONTROLLER_IMAGE ?= quay.io/skupper/service-controller
SERVICE_CONTROLLER_VERSION ?= 1.0.1
docker-build-service-controller:
	docker build $(ADD_ARGS) -t $(IMG_PREFIX)/skupper-service-controller:$(IMG_TAG) -f docker/Dockerfile \
	--build-arg BASE_IMAGE=$(SERVICE_CONTROLLER_IMAGE):$(SERVICE_CONTROLLER_VERSION) \
	--build-arg COMMAND=/app/service-controller \
	--build-arg COMPONENT_NAME=skupper-service-controller \
	./docker

CONFIG_SYNC_IMAGE ?= quay.io/skupper/config-sync
CONFIG_SYNC_VERSION ?= 1.0.1
docker-build-config-sync:
	docker build $(ADD_ARGS) -t $(IMG_PREFIX)/skupper-config-sync:$(IMG_TAG) -f docker/Dockerfile \
	--build-arg BASE_IMAGE=$(CONFIG_SYNC_IMAGE):$(CONFIG_SYNC_VERSION) \
	--build-arg COMMAND=/app/config-sync \
	--build-arg COMPONENT_NAME=skupper-config-sync \
	./docker

ROUTER_IMAGE ?= quay.io/skupper/skupper-router
ROUTER_VERSION ?= 2.0.2
docker-build-router:
	docker build $(ADD_ARGS) -t $(IMG_PREFIX)/skupper-router:$(IMG_TAG) -f docker/Dockerfile \
	--build-arg BASE_IMAGE=$(ROUTER_IMAGE):$(ROUTER_VERSION) \
	--build-arg COMMAND=/home/skrouterd/bin/launch.sh \
	--build-arg COMPONENT_NAME=skupper-router \
	./docker

docker-push: docker-push-site-controller docker-push-service-controller docker-push-config-sync docker-push-router

docker-push-site-controller:
	docker push $(IMG_PREFIX)/skupper-site-controller:$(IMG_TAG)

docker-push-service-controller:
	docker push $(IMG_PREFIX)/skupper-service-controller:$(IMG_TAG)

docker-push-config-sync:
	docker push $(IMG_PREFIX)/skupper-config-sync:$(IMG_TAG)

docker-push-router:
	docker push $(IMG_PREFIX)/skupper-router:$(IMG_TAG)
