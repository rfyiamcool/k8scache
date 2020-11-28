package k8scache

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type Listener interface {
	Receive() chan interface{}
	Notify(interface{})
	IsDone() bool
	Stop()
}

type ListenerPool struct {
	sync.Mutex
	data map[Listener]bool
}

func (lp *ListenerPool) get() {
	lp.Lock()
	defer lp.Unlock()
}

func (lp *ListenerPool) add(lis Listener) {
	lp.Lock()
	defer lp.Unlock()

	lp.data[lis] = true
}

func (lp *ListenerPool) del(lis Listener) {
	lp.Lock()
	defer lp.Unlock()

	delete(lp.data, lis)
}

func (lp *ListenerPool) copy() map[Listener]bool {
	lp.Lock()
	defer lp.Unlock()

	ndata := make(map[Listener]bool, len(lp.data))
	for k, v := range lp.data {
		ndata[k] = v
	}
	return ndata
}

type AnyListener struct {
	// todo
}

type PodListener struct {
	Name      string
	Namespace string
	isDone    bool
	deadline  *time.Timer

	sync.Mutex
	queue     chan interface{}
	equalFunc func(interface{}) bool
}

func NewPodListener(ns, name string, ufunc func(interface{}) bool) *PodListener {
	lis := &PodListener{
		Name:      name,
		Namespace: ns,
		queue:     make(chan interface{}, 5),
		equalFunc: ufunc,
	}
	lis.deadline = time.AfterFunc(60*time.Second, func() {
		lis.Stop()
	})
	return lis
}

func (p *PodListener) equal(obj interface{}) bool {
	if p.equalFunc != nil {
		return p.equalFunc(obj)
	}

	var (
		ns   string
		name string
	)
	switch obj.(type) {
	case *corev1.Pod:
		pod := obj.(*corev1.Pod)
		ns = pod.Namespace
		name = pod.Name
	}

	if ns == p.Namespace && name == p.Name {
		return true
	}
	return false
}

func (p *PodListener) IsDone() bool {
	return p.isDone
}

func (p *PodListener) Receive() chan interface{} {
	return p.queue
}

func (p *PodListener) Notify(ev interface{}) {
	p.Lock()
	defer p.Unlock()

	if p.isDone {
		return
	}

	select {
	case p.queue <- ev:
	default:
		p.isDone = true
		close(p.queue)
	}
}

func (p *PodListener) Stop() {
	p.Lock()
	defer p.Unlock()

	if p.isDone {
		return
	}
	p.isDone = true
	close(p.queue)
}
