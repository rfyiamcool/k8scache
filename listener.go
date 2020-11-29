package k8scache

import (
	"errors"
	"sync"
	"time"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

var (
	ErrFullQueue    = errors.New("channel is full")
	ErrCauseTimeout = errors.New("timeout")
)

type Listener interface {
	Receive() chan interface{}
	Notify(interface{})
	IsDone() bool
	Equal(interface{}) bool
	Stop()
}

type ListenerPool struct {
	sync.Mutex

	data map[string]map[Listener]bool // all resources
}

func newListenerPool() *ListenerPool {
	return &ListenerPool{
		data: make(map[string]map[Listener]bool, 10),
	}
}

func (lp *ListenerPool) add(genre string, lis Listener) error {
	lp.Lock()
	defer lp.Unlock()

	if !isSupportedResource(genre) {
		return ErrNotSupportResource
	}

	listeners, ok := lp.data[genre]
	if !ok {
		listeners = make(map[Listener]bool, 5)
	}
	listeners[lis] = true
	lp.data[genre] = listeners
	return nil
}

func (lp *ListenerPool) del(genre string, lis Listener) {
	lp.Lock()
	defer lp.Unlock()

	listeners, ok := lp.data[genre]
	if !ok {
		return
	}
	delete(listeners, lis)
}

func (lp *ListenerPool) copy(genre string) map[Listener]bool {
	lp.Lock()
	defer lp.Unlock()

	listeners, ok := lp.data[genre]
	if !ok {
		return map[Listener]bool{}
	}

	ndata := make(map[Listener]bool, len(listeners))
	for k, v := range listeners {
		ndata[k] = v
	}
	return ndata
}

func (lp *ListenerPool) xrange(genre string, obj interface{}) {
	for lis, _ := range lp.copy(genre) { // reduce lock
		if lis.IsDone() {
			lp.del(genre, lis)
			continue
		}
		if !lis.Equal(obj) {
			continue
		}

		lis.Notify(obj)
	}
}

type AnyListener struct {
	Name      string
	Namespace string
	isDone    bool
	deadline  *time.Timer
	err       error

	sync.Mutex
	queue     chan interface{}
	equalFunc func(interface{}) bool
}

// NewAnyListener if ns and name are null, always return true.
func NewAnyListener(ns, name string, uequal func(interface{}) bool) *AnyListener {
	lis := &AnyListener{
		Name:      name,
		Namespace: ns,
		queue:     make(chan interface{}, 5),
		equalFunc: uequal,
	}
	lis.deadline = time.AfterFunc(60*time.Second, func() {
		lis.Stop()
		if lis.err == nil {
			lis.err = ErrCauseTimeout
		}
	})
	return lis
}

func (p *AnyListener) Equal(obj interface{}) bool {
	if p.equalFunc != nil {
		return p.equalFunc(obj)
	}
	if p.Namespace == "" && p.Name == "" { // all match
		return true
	}
	if p.Namespace != "" && p.Name == "" { // match all resource in namespace
		return true
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

	case *corev1.Service:
		srv := obj.(*corev1.Service)
		ns = srv.Namespace
		name = srv.Name

	case *v1.Deployment:
		dm := obj.(*v1.Deployment)
		ns = dm.Namespace
		name = dm.Name

	case *v1.ReplicaSet:
		value := obj.(*v1.ReplicaSet)
		ns = value.Namespace
		name = value.Name

	case *v1.StatefulSet:
		value := obj.(*v1.StatefulSet)
		ns = value.Namespace
		name = value.Name

	case *v1.DaemonSet:
		value := obj.(*v1.DaemonSet)
		ns = value.Namespace
		name = value.Name

	case *corev1.Event:
		value := obj.(*corev1.Event)
		ns = value.Namespace
		name = value.Name

	case *corev1.Endpoints:
		value := obj.(*corev1.Endpoints)
		ns = value.Namespace
		name = value.Name
	}

	if ns != "" && name == "" { // match all resource in namespace
		return true
	}

	if ns == p.Namespace && name == p.Name {
		return true
	}
	return false
}

func (p *AnyListener) Error() error {
	return p.err
}

func (p *AnyListener) IsDone() bool {
	return p.isDone
}

func (p *AnyListener) Receive() chan interface{} {
	return p.queue
}

func (p *AnyListener) Notify(ev interface{}) {
	p.Lock()
	defer p.Unlock()

	if p.isDone {
		return
	}

	select {
	case p.queue <- ev:
	default:
		p.isDone = true
		p.err = ErrFullQueue
		close(p.queue)
	}
}

func (p *AnyListener) Stop() {
	p.Lock()
	defer p.Unlock()

	if p.isDone {
		return
	}
	p.isDone = true
	close(p.queue)
}
