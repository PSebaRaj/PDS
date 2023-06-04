package kv

import "hash/fnv"

// for shared utils for kv/server.go and kv/client.go

func GetShardForKey(key string, numShards int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())%numShards + 1
}
