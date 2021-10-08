#!/usr/bin/env bash

docker run --read-only --rm --user $(id -u $USER) --env PROJECT_VERSION="$1" --volume "$2":/usr/src/project "$3" /bin/bash -c "$4" "$2" compile
