#! /bin/bash

tag=$(date +%s%N)
echo "building destination mongo connector with tag: $tag"

docker build . -t bingo/destination-mongo:${tag}

echo "pushed ${tag}"
