#!/bin/bash -e
version=$1

docker build -t entsupml/unison-runner-dqd-omop-5.4:$version .
docker push entsupml/unison-runner-dqd-omop-5.4:$version
