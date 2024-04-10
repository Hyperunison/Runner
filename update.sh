#!/bin/bash

list=$(docker-compose exec -T unison-agent bash -c "ps -Af|grep 'python main.py for run_id'|grep -v grep")

if [ ! "$list" == "" ]; then
  echo "ERROR: can't update, nextflow process in progress:"
  echo $list
  exit 1
fi

if [ ! "$1" == "--force" ]; then
  git fetch
  newCommits=$(git log HEAD..origin/main --oneline)
  if [ "$newCommits" == "" ]; then
    echo "No new commits found, skip update"
    exit 2
  fi
  echo "Found new commits "
  echo $newCommits
  git pull
fi

docker-compose build
docker-compose down --remove-orphans
docker-compose up -d

echo "OK"