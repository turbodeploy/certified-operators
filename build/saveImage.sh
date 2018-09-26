#!/bin/bash

ARTIFACT_ID=$1
ARTIFACT_VERSION=$2
TARGET_DIR=$3

echo "Tagging docker image"
docker tag turbonomic/${ARTIFACT_ID}:${ARTIFACT_VERSION} localhost:5000/turbonomic/${ARTIFACT_ID}:${ARTIFACT_VERSION}
echo "Saving docker image locally"
docker save localhost:5000/turbonomic/${ARTIFACT_ID}:${ARTIFACT_VERSION} | xz -T0 -9 > ${TARGET_DIR}/${ARTIFACT_ID}-${ARTIFACT_VERSION}.tgz
