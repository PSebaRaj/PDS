package kv

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/psebaraj/pds/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	mu sync.RWMutex

	shardsActive map[int]bool
	shardLocks   []sync.RWMutex
	shardStores  []map[string]storeType
}

type storeType struct {
	value string
	ttl   uint64
}

func (server *KvServerImpl) handleShardMapUpdate() {
	server.mu.Lock()
	updatedShards := server.listener.shardMap.ShardsForNode(server.nodeName)

	prevShards := server.shardsActive
	newShards := make(map[int]bool, 0)

	for i := 0; i < len(updatedShards); i++ {
		newShards[updatedShards[i]] = true
	}

	nodesToAdd := make([]int, 0)

	for node := range newShards {
		if !prevShards[node] {
			nodesToAdd = append(nodesToAdd, node)
		}
	}

	for i := 0; i < len(nodesToAdd); i++ {
		shard := nodesToAdd[i]
		nodes := server.shardMap.GetState().ShardsToNodes[shard]

		randIdx := rand.Intn(len(nodes)) // for load balancing
		var lastSeenErr error

		for k := 0; k < len(nodes); k++ {
			currNode := nodes[(randIdx+k)%len(nodes)] // lb

			if currNode != server.nodeName { // avoid connecting to itself...
				client, err := server.clientPool.GetClient(currNode)

				if err != nil {
					lastSeenErr = err
					logrus.Tracef("Unable to get client for node %s; called from node %s", currNode, server.nodeName)

					continue
				}

				server.mu.Unlock()
				updatedKVsResp, err := client.GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: int32(shard)})
				server.mu.Lock()

				if err != nil {
					lastSeenErr = err
					logrus.Tracef("Client attempted GetShardContexts call; unable to fetch contents on shard on node %s; called from node %s", currNode, server.nodeName)

					continue
				}

				server.shardLocks[shard-1].Lock()

				updatedKVs := updatedKVsResp.Values

				for j := 0; j < len(updatedKVs); j++ {
					key := updatedKVs[j].Key
					value := updatedKVs[j].Value
					ttl := updatedKVs[j].TtlMsRemaining

					server.shardStores[shard-1][key] = storeType{value: value, ttl: uint64(ttl)}
				}

				server.shardLocks[shard-1].Unlock()
			}
		}

		if lastSeenErr != nil {
			logrus.Tracef("Client unavailable")
		}
	}

	server.shardsActive = newShards
	server.mu.Unlock()

	nodesToDelete := make([]int, 0)

	for node := range prevShards {
		if !newShards[node] {
			nodesToDelete = append(nodesToDelete, node)
		}
	}

	for i := 0; i < len(nodesToDelete); i++ {
		shard := nodesToDelete[i]
		server.shardLocks[shard-1].Lock()

		for key := range server.shardStores[shard-1] {
			keyOnWhichShard := GetShardForKey(key, server.shardMap.NumShards())

			if keyOnWhichShard == shard {
				delete(server.shardStores[shard-1], key)
			}
		}

		server.shardLocks[shard-1].Unlock()
	}
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:     nodeName,
		shardMap:     shardMap,
		listener:     &listener,
		clientPool:   clientPool,
		shutdown:     make(chan struct{}),
		shardsActive: make(map[int]bool),
		shardLocks:   make([]sync.RWMutex, shardMap.NumShards()),
		shardStores:  make([]map[string]storeType, shardMap.NumShards()),
	}

	for i := 0; i < len(server.shardStores); i++ {
		server.shardStores[i] = make(map[string]storeType)
	}

	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	go server.CleanShards()
	return &server
}

func (server *KvServerImpl) CleanShards() {
	for {
		time.Sleep(1 * time.Second)
		server.mu.Lock()

		for i := 0; i < len(server.shardStores); i++ {
			server.shardLocks[i].Lock()

			for key, value := range server.shardStores[i] {
				if value.ttl < uint64(time.Now().UnixMilli()) {
					delete(server.shardStores[i], key)
				}
			}

			server.shardLocks[i].Unlock()
		}

		server.mu.Unlock()
	}
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.listener.Close()
}

func (server *KvServerImpl) isCorrectShard(key string) bool {
	server.mu.RLock()
	defer server.mu.RUnlock()

	shardIdx := GetShardForKey(key, server.shardMap.NumShards())
	isActive := server.shardsActive[shardIdx]

	return isActive
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	if request.Key == "" {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.InvalidArgument, "Key is the empty string")
	}

	if !server.isCorrectShard(request.Key) {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.NotFound, "Key is not located on active shard")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.shardLocks[shard-1].RLock()
	defer server.shardLocks[shard-1].RUnlock()

	value, status := server.shardStores[shard-1][request.Key]

	if status {
		if value.ttl < uint64(time.Now().UnixMilli()) { // expired
			logrus.Tracef("Found EXPIRED entry with key: %v; value: %v; ttl: %v", request.Key, value.value, value.ttl)
			return &proto.GetResponse{Value: "", WasFound: false}, nil
		} else {
			logrus.Tracef("Found entry with key: %v; value: %v; ttl: %v", request.Key, value.value, value.ttl)
			return &proto.GetResponse{Value: value.value, WasFound: true}, nil

		}
	} else {
		logrus.Tracef("Did NOT find entry with key: %v", request.Key)
		return &proto.GetResponse{Value: "", WasFound: false}, nil
	}
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key is the empty string")
	}

	if !server.isCorrectShard(request.Key) {
		return nil, status.Error(codes.NotFound, "Key is not located on active shard")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.shardLocks[shard-1].Lock()

	var value storeType // blind write
	value.value = request.Value
	value.ttl = uint64(time.Now().UnixMilli()) + uint64(request.TtlMs)

	server.shardStores[shard-1][request.Key] = value

	server.shardLocks[shard-1].Unlock()

	logrus.Tracef("Successfully set entry with key: %v; value: %v; ttl: %v", request.Key, request.Value, request.TtlMs)

	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key is the empty string")
	}

	if !server.isCorrectShard(request.Key) {
		return nil, status.Error(codes.NotFound, "Key is not located on active shard")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.shardLocks[shard-1].Lock()

	delete(server.shardStores[shard-1], request.Key)

	server.shardLocks[shard-1].Unlock()

	logrus.Tracef("Successfully deleted entry with key: %v", request.Key)

	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) Add(
	ctx context.Context,
	request *proto.AddRequest,
) (*proto.AddResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Add() request")

	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key is the empty string")
	}

	if !server.isCorrectShard(request.Key) {
		return nil, status.Error(codes.NotFound, "Key is not located on active shard")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.shardLocks[shard-1].Lock()

	_, exists := server.shardStores[shard-1][request.Key]

	if exists == false { // Add does not write when key already exists
		var value storeType
		value.value = request.Value
		value.ttl = uint64(time.Now().UnixMilli()) + uint64(request.TtlMs)

		server.shardStores[shard-1][request.Key] = value

		server.shardLocks[shard-1].Unlock()

		logrus.Tracef("Successfully added entry with key: %v; value: %v; ttl: %v", request.Key, request.Value, request.TtlMs)
	} else { // exists in, so do nothing
		server.shardLocks[shard-1].Unlock()

		logrus.Tracef("Could not add entry with key: %v; value: %v; ttl: %v; as ENTRY ALREADY EXISTS", request.Key, request.Value, request.TtlMs)
	}

	return &proto.AddResponse{}, nil
}

func (server *KvServerImpl) Replace(
	ctx context.Context,
	request *proto.ReplaceRequest,
) (*proto.ReplaceResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Replace() request")

	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key is the empty string")
	}

	if !server.isCorrectShard(request.Key) {
		return nil, status.Error(codes.NotFound, "Key is not located on active shard")
	}

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.shardLocks[shard-1].Lock()

	_, exists := server.shardStores[shard-1][request.Key]

	if exists == true { // Replace does not write when key is not already in store
		var value storeType
		value.value = request.Value
		value.ttl = uint64(time.Now().UnixMilli()) + uint64(request.TtlMs)

		server.shardStores[shard-1][request.Key] = value

		server.shardLocks[shard-1].Unlock()

		logrus.Tracef("Successfully replaced entry with key: %v; value: %v; ttl: %v", request.Key, request.Value, request.TtlMs)
	} else { // does not exist in, so do nothing
		server.shardLocks[shard-1].Unlock()

		logrus.Tracef("Could not replace entry with key: %v; value: %v; ttl: %v; as ENTRY DOES NOT ALREADY EXISTS", request.Key, request.Value, request.TtlMs)
	}

	return &proto.ReplaceResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	shard := request.Shard

	server.shardLocks[shard-1].RLock()
	defer server.shardLocks[shard-1].RUnlock()

	contents := make([]*proto.GetShardValue, 0)

	for key, value := range server.shardStores[shard-1] {
		contents = append(contents, &proto.GetShardValue{Key: key, Value: value.value, TtlMsRemaining: int64(value.ttl)})
	}

	return &proto.GetShardContentsResponse{Values: contents}, nil

}
