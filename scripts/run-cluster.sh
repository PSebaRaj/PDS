#!/bin/bash

# Run N instances of the K/V server based on a given shardmap
# Usage: run-cluster.sh shardmap.json

set -e

shardmap_file=$1
nodes=$(jq -r '.nodes | keys[]' < $shardmap_file)
for node in $nodes ; do
	go run cmd/server/server.go --shardmap "$shardmap_file" --node "$node" &
done

wait
