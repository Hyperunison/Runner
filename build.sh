#!/bin/bash -e
version=$1
if [ -z "$version" ]; then
  echo "Usage bash build.sh <version>"
  exit 1
fi

echo "==$version=="
docker build -t unison-runner_intermediate -f Resources/docker/Dockerfile .
docker build -t entsupml/unison-runner:$version -f Resources/docker/Dockerfile_full .
#docker push entsupml/unison-runner:$version
