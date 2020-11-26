package k8scache

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimout(t *testing.T) {
	cc, err := NewClusterCache(nil)
	assert.Equal(t, err, nil)

	start := time.Now()
	done, _ := cc.timeoutChan(1 * time.Second)
	select {
	case <-done:
	}
	assert.Less(t, time.Since(start).Seconds(), float64(3))

	start2 := time.Now()
	done2, _ := cc.timeoutChan(0)
	select {
	case <-done2:
	case <-time.After(1 * time.Second):
	}
	assert.Less(t, float64(0), time.Since(start2).Seconds())
}

func TestNotReady(t *testing.T) {
	cc, err := NewClusterCache(nil)
	assert.Equal(t, err, nil)

	_, err = cc.GetNamespaces()
	assert.Equal(t, err, ErrCacheNotReady)
}

func newTestClient(t *testing.T) *ClusterCache {
	cli, err := NewClient()
	assert.Equal(t, err, nil)

	err = cli.BuildForKubecfg()
	assert.Equal(t, err, nil)

	cc, err := NewClusterCache(cli)
	assert.Equal(t, err, nil)
	return cc
}

func TestSimple(t *testing.T) {
	cc := newTestClient(t)
	pods, err := cc.ListPods("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(pods.Items), 0)

	cc.Start()
	cc.SyncCache()

	// evs, err := cc.ListEvents("default")

	evs, err := cc.GetAllEvents("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(evs), 0)
	bs, err := json.Marshal(evs)
	fmt.Println(string(bs))

	ends, err := cc.GetAllEndpoints("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(ends), 0)

	nslist, err := cc.GetNamespaces()
	assert.Equal(t, err, nil)
	assert.Greater(t, len(nslist), 0)
	// fmt.Printf("namespace list %v \n\n", nslist)

	nodes, err := cc.GetNodes()
	assert.Equal(t, err, nil)
	assert.Greater(t, len(nodes), 0)
	// fmt.Printf("nodes list %v \n\n", nodes)

	mpods, err := cc.GetAllPods("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(mpods), 0)
	// fmt.Printf("default pods list %v \n\n", mpods)

	svcs, err := cc.GetAllServices("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(svcs), 0)
	// fmt.Printf("default services list %v \n\n", svcs)

	reps, err := cc.GetAllReplicas("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(reps), 0)
	// fmt.Printf("default replicas list %v \n\n", reps)

	dms, err := cc.GetAllDeployments("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(dms), 0)
	// fmt.Printf("default deployments list %v \n\n", dms)

	time.AfterFunc(1*time.Second, func() {
		cc.Stop()
	})
	cc.Wait()
}
