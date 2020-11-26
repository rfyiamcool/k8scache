package k8scache

import (
	"sync"
)

type ClusterCachePool struct {
	sync.RWMutex
	data map[string]*ClusterCache // key: k8s cluster
}

func NewClusterCachePool() *ClusterCachePool {
	return &ClusterCachePool{
		data: make(map[string]*ClusterCache, 2),
	}
}

func (cp *ClusterCachePool) Set(id string, cc *ClusterCache) {
	cp.Lock()
	defer cp.Unlock()

	cp.data[id] = cc
}

func (cp *ClusterCachePool) Get(id string) (*ClusterCache, bool) {
	cp.RLock()
	defer cp.RUnlock()

	val, ok := cp.data[id]
	return val, ok
}
