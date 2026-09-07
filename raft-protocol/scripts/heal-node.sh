#!/usr/bin/env sh
set -eu

node="${1:-}"
case "$node" in
    node1) node_ip="172.30.0.11" ;;
    node2) node_ip="172.30.0.12" ;;
    node3) node_ip="172.30.0.13" ;;
    *)
        echo "Usage: $0 <node1|node2|node3>" >&2
        exit 2
        ;;
esac

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_dir=$(dirname -- "$script_dir")
container_id=$(docker compose --profile full --project-directory "$project_dir" \
    -f "$project_dir/docker-compose.yaml" ps -q "$node")

if [ -z "$container_id" ]; then
    echo "$node is not running" >&2
    exit 1
fi

connected=$(docker inspect --format \
    '{{if index .NetworkSettings.Networks "raft-cluster"}}yes{{end}}' "$container_id")

if [ "$connected" = "yes" ]; then
    echo "$node is already connected to raft-cluster"
    exit 0
fi

docker network connect --ip "$node_ip" raft-cluster "$container_id"
echo "$node is reconnected to raft-cluster at $node_ip"

