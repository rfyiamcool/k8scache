## K8SCACHE

local cache for kubernetes apiserver cache

`k8scache is based on client-go informer`

### Feature

**support resource list:**

- namespace
- pod
- node
- service 
- replicaSet
- deployment
- daemonSet
- statefulSet
- event 

### Usage

```go
func TestSimple(t *testing.T) {
	cli, err := NewClient()
	assert.Equal(t, err, nil)

	err = cli.BuildForKubecfg()
	assert.Equal(t, err, nil)

	cc, err := NewClusterCache(cli)
	assert.Equal(t, err, nil)

	pods, err := cc.ListPods()
	assert.Equal(t, err, nil)
	assert.Greater(t, len(pods.Items), 0)

	cc.Start()
	cc.SyncCache()

	nslist, err := cc.GetNamespaces()
	assert.Equal(t, err, nil)
	assert.Greater(t, len(nslist), 0)
	fmt.Printf("namespace list %v \n\n", nslist)

	nodes, err := cc.GetNodes()
	assert.Equal(t, err, nil)
	assert.Greater(t, len(nodes), 0)
	fmt.Printf("nodes list %v \n\n", nodes)

	mpods, err := cc.GetPodsWithNS("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(mpods), 0)
	fmt.Printf("default pods list %v \n\n", mpods)

	svcs, err := cc.GetServicesWithNS("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(svcs), 0)
	fmt.Printf("default services list %v \n\n", svcs)

	reps, err := cc.GetReplicasWithNS("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(reps), 0)
	fmt.Printf("default replicas list %v \n\n", reps)

	dms, err := cc.GetDeploymentsWithNS("default")
	assert.Equal(t, err, nil)
	assert.Greater(t, len(dms), 0)
	fmt.Printf("default deployments list %v \n\n", dms)

	time.AfterFunc(1*time.Second, func() {
		cc.Stop()
	})
    cc.Wait()
}
```