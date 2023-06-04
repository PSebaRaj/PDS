package kvtest

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/psebaraj/pds/kv"
	"github.com/stretchr/testify/assert"
)


func TestYourTest1(t *testing.T) {
	assert.True(t, true)
}

// Client Test - Load Balancing Get Requests (4 nodes, 1 shard)
func TestClientGetLoadBalance(t *testing.T) {

	setup := MakeTestSetupWithoutServers(kv.ShardMapState{
		NumShards: 1,
		Nodes:     makeNodeInfos(4),
		ShardsToNodes: map[int][]string{
			1: {"n1", "n2", "n3", "n4"},
		},
	})

	setup.clientPool.OverrideGetResponse("n1", "val", true)
	setup.clientPool.OverrideGetResponse("n2", "val", true)
	setup.clientPool.OverrideGetResponse("n3", "val", true)
	setup.clientPool.OverrideGetResponse("n4", "val", true)

	for i := 0; i < 100; i++ {
		value, wasFound, err := setup.Get("abc")
		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, "val", value)
	}

	n1Rpc := setup.clientPool.GetRequestsSent("n1")
	assert.Less(t, 17, n1Rpc)
	assert.Greater(t, 33, n1Rpc)

	n2Rpc := setup.clientPool.GetRequestsSent("n2")
	assert.Less(t, 17, n2Rpc)
	assert.Greater(t, 33, n2Rpc)

	n3Rpc := setup.clientPool.GetRequestsSent("n3")
	assert.Less(t, 17, n3Rpc)
	assert.Greater(t, 33, n3Rpc)

	n4Rpc := setup.clientPool.GetRequestsSent("n4")
	assert.Less(t, 17, n4Rpc)
	assert.Greater(t, 33, n4Rpc)

}

// Integration Test - Concurrent Get, Set, and (extraneous) Delete
func TestIntegrationConcurrentGetsSetsDeletes(t *testing.T) {
	setup := MakeTestSetup(MakeFourNodesWithFiveShards())

	const numGoros = 30
	const numIters = 500

	keys := RandomKeys(200, 20)
	vals := RandomKeys(200, 40)

	var numDeletesRequested int32 = 0

	found := make([]int32, 200)
	deleted := make([]int32, 200)

	var wg sync.WaitGroup

	wg.Add(numGoros)
	for i := 0; i < numGoros; i++ {
		go func(i int) {
			for j := 0; j < numIters; j++ {
				err := setup.Set(keys[(i*100+j)%150], vals[(j*100+i)%150], 100*time.Second)
				assert.Nil(t, err)

				for k := 0; k < 200; k++ {
					_, wasFound, err := setup.Get(keys[k])
					assert.Nil(t, err)

					if wasFound {
						atomic.StoreInt32(&found[k], 1)
					}
				}
				err = setup.Delete(keys[(i*100+j)%150])

				if err == nil {
					atomic.StoreInt32(&deleted[(i*100+j)%150], 1)
					atomic.AddInt32(&numDeletesRequested, 1)
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	for i := 0; i < 150; i++ {
		assert.True(t, found[i] == 1)
	}
	for i := 150; i < 200; i++ {
		assert.False(t, found[i] == 1)
	}

	// Even though 15,000 deletes are requested, only the correct entries should be deleted
	assert.True(t, numDeletesRequested == 15000)

	for i := 0; i < 150; i++ {
		assert.True(t, deleted[i] == 1)
	}
	for i := 150; i < 200; i++ {
		assert.False(t, deleted[i] == 1)
	}


	setup.Shutdown()
}

func TestSingleShardCopyNodeToNode(t *testing.T) {

	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 1,
			Nodes:     makeNodeInfos(2),
			ShardsToNodes: map[int][]string{
				1: {"n1"},
			},
		},
	)

	keys := RandomKeys(1000, 20)

	for _, key := range keys {
		err := setup.NodeSet("n1", key, "cs426 is my fav class", 3*time.Second)
		assert.Nil(t, err)
	}

	setup.UpdateShardMapping(map[int][]string{
		1: {"n1", "n2"},
	})

	setup.UpdateShardMapping(map[int][]string{
		1: {"n2"},
	})

	for _, key := range keys {
		_, _, err := setup.NodeGet("n1", key)
		assertShardNotAssigned(t, err)
	}

	for _, key := range keys {
		value, wasFound, err := setup.NodeGet("n2", key)

		assert.Nil(t, err)
		assert.True(t, wasFound)
		assert.Equal(t, value, "cs426 is my fav class")
	}

	setup.Shutdown()
}

// Server Test - simply checking for invalid ttl
func TestServerExpiredTTL(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	for i := 0; i < 100; i++ {

		ttl := rand.Intn(250)
		sleepTime := rand.Intn(250)

		err := setup.NodeSet("n1", "abc", "123", time.Duration(ttl)*time.Millisecond)
		assert.Nil(t, err)

		time.Sleep(time.Duration(sleepTime) * time.Millisecond)

		if sleepTime > ttl {
			_, wasFound, err := setup.NodeGet("n1", "abc")

			assert.Nil(t, err)
			assert.False(t, wasFound)
		} else {
			val, wasFound, err := setup.NodeGet("n1", "abc")

			assert.Nil(t, err)
			assert.True(t, wasFound)
			assert.Equal(t, "123", val)
		}
	}
	assert.True(t, true)

	setup.Shutdown()
}

// Server Test - checking for invalid ttl after moving shards, even after update
func TestServerExpiredTTLMovingShards(t *testing.T) {
	setup := MakeTestSetup(MakeTwoNodeMultiShard())

	keys := RandomKeys(200, 20)
	ogKeys := make(map[string]string)

	for _, key := range keys {
		err := setup.NodeSet("n1", key, "mooooooooo", 1*time.Second)

		if err == nil {
			ogKeys[key] = "n1"
		} else {
			assertShardNotAssigned(t, err)
			err = setup.NodeSet("n2", key, "123", 5*time.Second)

			assert.Nil(t, err)
			ogKeys[key] = "n2"
		}
	}

	setup.UpdateShardMapping(map[int][]string{
		1: {"n1", "n2"},
		2: {"n1", "n2"},
	})

	setup.UpdateShardMapping(map[int][]string{
		1: {"n2"},
		2: {"n1"},
	})

	time.Sleep(1 * time.Second)

	for _, key := range keys {
		node := ""

		if ogKeys[key] == "n1" {
			node = "n2"
		} else {
			node = "n1"
		}

		// it should not find anything here...
		_, wasFound, _ := setup.NodeGet(node, key)
		assert.False(t, wasFound)
	}

	setup.Shutdown()

}
