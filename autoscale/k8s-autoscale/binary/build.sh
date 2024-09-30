#!/bin/bash
set -e

cd ../..

function pull_base_images {
    docker pull debian:bookworm-slim
    docker pull golang:1-bookworm
}

pull_base_images

docker buildx build -f ./k8s-autoscale/binary/Dockerfile . --tag jot85/redis-work-queue-k8s-autoscale:0.3.2
docker push jot85/redis-work-queue-k8s-autoscale:0.3.2
