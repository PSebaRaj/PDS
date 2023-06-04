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

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}

	return kv
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.GetState().ShardsToNodes[shard]

	if len(nodes) == 0 {
		return "", false, status.Errorf(codes.NotFound, "Unable to find any nodes hosting shard %v", shard)
	}

	randIdx := rand.Intn(len(nodes))
	var lastSeenErr error

	for i := 0; i < len(nodes); i++ {
		currNode := nodes[(randIdx+i)%len(nodes)] // lb
		client, err := kv.clientPool.GetClient(currNode)

		if err != nil {
			lastSeenErr = err
			logrus.Tracef("KvClient error; node %v", currNode)

			continue
		}

		resp, err := client.Get(ctx, &proto.GetRequest{Key: key})

		if err != nil {
			lastSeenErr = err
			logrus.Tracef("Client Get request failed; node %v, key %v", currNode, key)

			continue
		}

		if !resp.WasFound {
			logrus.Tracef("Get() call successful; did not find key: %s", key)
			return "", false, nil
		}

		// unreached if all nodes error
		logrus.Tracef("Get() call successful; found key: %s", key)
		return resp.Value, true, nil
	}

	// reached ONLY if all nodes storing copy of shard produce errors
	logrus.Tracef("Get() call unsuccessful; error generated on all nodes housing the shard of key: %s", key)
	return "", false, lastSeenErr
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.GetState().ShardsToNodes[shard]

	if len(nodes) == 0 {
		return status.Errorf(codes.NotFound, "Unable to find any nodes hosting shard %v", shard)
	}

	errorCh := make(chan error, len(nodes)+1)
	randIdx := rand.Intn(len(nodes))

	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		wg.Add(1)

		go func(i int, ch chan error) {
			defer wg.Done()

			currNode := nodes[(randIdx+i)%len(nodes)] // lb
			client, err := kv.clientPool.GetClient(currNode)

			if err != nil || client == nil {
				ch <- status.Errorf(codes.NotFound, "KvClient creation failed on node %v", currNode)
				return
			}

			_, err = client.Set(ctx, &proto.SetRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()})

			if err != nil {
				ch <- status.Errorf(codes.Unavailable, "Error setting key %v; value %v on node %v", key, value, currNode)
			}
		}(i, errorCh)
	}

	wg.Wait()

	if len(errorCh) > 0 {
		err := <-errorCh
		logrus.Tracef("Set() call unsuccessful, error: %v; wrote entry key: %s; value: %s; ttl: %v", err, key, value, ttl.Milliseconds())
		return err
	}

	logrus.Tracef("Set() call successful; wrote entry key: %s; value: %s; ttl: %v", key, value, ttl.Milliseconds())
	return nil
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.GetState().ShardsToNodes[shard]

	if len(nodes) == 0 {
		return status.Errorf(codes.NotFound, "Unable to find any nodes hosting shard %v", shard)
	}

	errorCh := make(chan error, len(nodes)+1)
	randIdx := rand.Intn(len(nodes))

	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		wg.Add(1)

		go func(i int, ch chan error) {
			defer wg.Done()

			currNode := nodes[(randIdx+i)%len(nodes)] // lb
			client, err := kv.clientPool.GetClient(currNode)

			if err != nil || client == nil {
				ch <- status.Errorf(codes.NotFound, "KvClient creation failed on node %v", currNode)
				return
			}

			_, err = client.Delete(ctx, &proto.DeleteRequest{Key: key})

			if err != nil {
				ch <- status.Errorf(codes.Unavailable, "Error deleting key %v on node %v", key, currNode)
			}
		}(i, errorCh)
	}

	wg.Wait()

	if len(errorCh) > 0 {
		err := <-errorCh
		logrus.Tracef("Delete() call unsuccessful, error: %v; wrote entry key: %s", err, key)
		return err
	}

	logrus.Tracef("Delete() call successful; deleted entry with key: %s", key)
	return nil
}
