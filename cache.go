package k8scache

import (
	"context"
	"errors"
	"path"
	"sync"
	"time"

	"github.com/mitchellh/go-homedir"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	TypeNamespace   = "Namespace"
	TypePod         = "Pod"
	TypeNode        = "Node"
	TypeService     = "Service"
	TypeReplicaSet  = "ReplicaSet"
	TypeDeployment  = "Deployment"
	TypeDaemonSet   = "DaemonSet"
	TypeStatefulSet = "StatefulSet"
	TypeEvent       = "Event"
)

var (
	ErrNotFoundNamespace   = errors.New("not found namespace")
	ErrNotFoundName        = errors.New("not found name")
	ErrSyncResourceTimeout = errors.New("sync resource timeout")
	ErrCacheNotReady       = errors.New("cache not ready, wait to sync full cache in the beginning")

	allResources = []string{
		TypeNamespace,
		TypePod,
		TypeNode,
		TypeService,
		TypeReplicaSet,
		TypeDeployment,
		TypeDaemonSet,
		TypeStatefulSet,
		TypeEvent,
	}
)

type Client struct {
	client *kubernetes.Clientset
}

func NewClient() (*Client, error) {
	return &Client{}, nil
}

func (cs *Client) GetClient() *kubernetes.Clientset {
	return cs.client
}

func (cs *Client) BuildForKubecfg() error {
	dir, err := homedir.Dir()
	if err != nil {
		return err
	}

	kubecfg := path.Join(dir, "/.kube/config")
	return cs.BuildForCustomKubecfg(kubecfg)
}

func (cs *Client) BuildForCustomKubecfg(kubecfg string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubecfg)
	if err != nil {
		return err
	}

	cs.client, err = kubernetes.NewForConfig(config)
	return err
}

func (cs *Client) BuildForInCluster(kubecfg string) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	cs.client, err = kubernetes.NewForConfig(config)
	return err
}

func (cs *Client) BuildForParams(addr, token, cert, key, ca string) error {
	var insecure bool
	if token != "" {
		insecure = true
	}

	var cfg = &rest.Config{
		Host:          addr,
		ContentConfig: rest.ContentConfig{},
		BearerToken:   token,
		Impersonate:   rest.ImpersonationConfig{},
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: insecure,
			CertData: []byte(cert),
			KeyData:  []byte(key),
			CAData:   []byte(ca),
		},
	}
	clientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	cs.client = clientSet
	return nil
}

type ClusterCachePool struct {
	data map[string]*ClusterCache
}

type OptionFunc func(*ClusterCache) error

func WithNamespace(ns string) OptionFunc {
	return func(o *ClusterCache) error {
		o.namespace = ns
		return nil
	}
}

func WithContext(ctx context.Context) OptionFunc {
	return func(o *ClusterCache) error {
		o.ctx = ctx
		return nil
	}
}

func WithStopper(stopper chan struct{}) OptionFunc {
	return func(o *ClusterCache) error {
		o.stopper = stopper
		return nil
	}
}

func WithFollowResource(rlist []string) OptionFunc {
	return func(o *ClusterCache) error {
		mm := map[string]bool{}
		for _, res := range rlist {
			mm[res] = true
		}
		o.followResource = mm // cover old val
		return nil
	}
}

func WithNotFollowResource(rlist []string) OptionFunc {
	return func(o *ClusterCache) error {
		for _, res := range rlist {
			delete(o.followResource, res)
		}
		return nil
	}
}

func WithFollowNamespaces(nlist []string) OptionFunc {
	return func(o *ClusterCache) error {
		mm := map[string]bool{}
		for _, ns := range nlist {
			mm[ns] = true
		}
		o.followNamespaces = mm // cover old val
		return nil
	}
}

func WithNotFollowNamespaces(nlist []string) OptionFunc {
	return func(o *ClusterCache) error {
		for _, ns := range nlist {
			delete(o.followNamespaces, ns)
		}
		return nil
	}
}

func NewClusterCache(client *Client, opts ...OptionFunc) (*ClusterCache, error) {
	cc := &ClusterCache{
		k8sClient:        client,
		stopper:          make(chan struct{}, 0),
		cache:            make(map[string]*DataSet, 5),
		nodes:            make(map[string]*corev1.Node, 5),
		namespaces:       make(map[string]*corev1.Namespace, 5),
		followResource:   getAllResources(),
		followNamespaces: make(map[string]bool, 0),
	}
	for _, opt := range opts {
		err := opt(cc)
		if err != nil {
			return cc, err
		}
	}
	return cc, nil
}

type ClusterCache struct {
	ctx              context.Context
	cancel           context.CancelFunc
	namespace        string
	k8sClient        *Client
	followResource   map[string]bool
	followNamespaces map[string]bool
	isReady          bool // todo: all resource -> map[resource]bool

	stopper  chan struct{}
	stopOnce sync.Once

	factory            informers.SharedInformerFactory
	namespaceInformer  cache.SharedIndexInformer
	podInformer        cache.SharedIndexInformer
	nodeInformer       cache.SharedIndexInformer
	serviceInformer    cache.SharedIndexInformer
	deploymentInformer cache.SharedIndexInformer
	replicaInformer    cache.SharedIndexInformer
	daemonInformer     cache.SharedIndexInformer
	statefulInformer   cache.SharedIndexInformer
	eventInformer      cache.SharedIndexInformer

	nodes      map[string]*corev1.Node
	namespaces map[string]*corev1.Namespace
	cache      map[string]*DataSet // key: namespace, val: *DataSet
	cacheMutex sync.RWMutex
}

func (c *ClusterCache) Start() {
	c.factory = informers.NewSharedInformerFactory(c.k8sClient.client, 0)
	c.bindResourceInformer()
	c.factory.Start(c.stopper)
}

func (c *ClusterCache) Wait() {
	select {
	case <-c.stopper:
	}
}

func (c *ClusterCache) Stop() {
	select {
	case <-c.stopper: // avoid close stopper (withStopper) ouside
		return
	default:
	}

	c.stopOnce.Do(func() {
		close(c.stopper)
	})
}

func (c *ClusterCache) Reset() {
	c.stopper = make(chan struct{}, 0)
	c.stopOnce = sync.Once{}
}

func (c *ClusterCache) isClosed() bool {
	select {
	case <-c.stopper:
		return true
	default:
		return false
	}
}

func (c *ClusterCache) isFollowResource(res string) bool {
	_, ok := c.followResource[res]
	return ok
}

func (c *ClusterCache) isFollowNamespace(ns string) bool {
	if len(c.followNamespaces) == 0 { // unset
		return true
	}

	_, ok := c.followNamespaces[ns]
	return ok
}

func (c *ClusterCache) bindResourceInformer() {
	c.bindNamespaceInformer()
	c.bindPodInformer()
	c.bindNodesInformer()
	c.bindReplicaInformer()
	c.bindServiceInformer()
	c.bindDeploymentInformer()
	c.bindStatefulInformer()
	c.bindDaemonInformer()
	c.bindEventInformer()
}

var defaultSyncTimeout = time.Duration(30 * time.Second)

func (c *ClusterCache) timeoutChan(ts time.Duration) (chan struct{}, *time.Timer) {
	done := make(chan struct{})
	if ts == 0 {
		return done, time.NewTimer(0)
	}

	timer := time.AfterFunc(ts, func() {
		close(done)
	})
	return done, timer
}

func (c *ClusterCache) SyncCache() error {
	return c.SyncCacheWithTimeout(defaultSyncTimeout)
}

func (c *ClusterCache) SyncCacheWithTimeout(timeout time.Duration) error {
	var (
		funcs = []func(time.Duration) error{}
		err   error
	)

	funcs = append(funcs,
		c.SyncNamespacesCache,
		c.SyncPodsCache,
		c.SyncNodesCache,
		c.SyncServicesCache,
		c.SyncDeploymentsCache,
		c.SyncReplicasCache,
		c.SyncDaemonCache,
		c.SyncStatefulCache,
		c.SyncEventsCache,
	)

	wg := sync.WaitGroup{} // concurrent sync
	for _, fn := range funcs {
		fn := fn // avoid go for range bug

		wg.Add(1)
		go func() {
			defer wg.Done()
			cerr := fn(timeout)
			if err != nil {
				err = cerr
			}
		}()
	}
	wg.Wait()

	if err == nil {
		c.isReady = true
	}
	return err
}

func (c *ClusterCache) bindNamespaceInformer() {
	c.namespaceInformer = c.factory.Core().V1().Namespaces().Informer()
	add := func(obj interface{}) {
		ns, ok := obj.(*corev1.Namespace)
		if !ok {
			return
		}
		if !c.isFollowNamespace(ns.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		c.namespaces[ns.Name] = ns
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		ns, ok := obj.(*corev1.Namespace)
		if !ok {
			return
		}
		if !c.isFollowNamespace(ns.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		delete(c.namespaces, ns.Name)
	}

	c.namespaceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncNamespacesCache(timeout time.Duration) error {
	if !c.isFollowResource(TypePod) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.namespaceInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindPodInformer() {
	if !c.isFollowResource(TypePod) {
		return
	}

	c.podInformer = c.factory.Core().V1().Pods().Informer()
	add := func(obj interface{}) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return
		}
		if !c.isFollowNamespace(pod.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[pod.Namespace]
		if !ok {
			data = newDataSet(pod.Namespace)
			c.cache[pod.Namespace] = data
		}

		data.upsertPod(pod)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return
		}
		if !c.isFollowNamespace(pod.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[pod.Namespace]
		if !ok {
			return
		}
		data.deletePod(pod)
	}

	c.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncPodsCache(timeout time.Duration) error {
	if !c.isFollowResource(TypePod) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.podInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindNodesInformer() {
	if !c.isFollowResource(TypeNode) {
		return
	}

	c.nodeInformer = c.factory.Core().V1().Nodes().Informer()
	add := func(obj interface{}) {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return
		}

		c.cacheMutex.Lock()
		c.nodes[node.Name] = node
		c.cacheMutex.Unlock()
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return
		}

		c.cacheMutex.Lock()
		delete(c.nodes, node.Name)
		c.cacheMutex.Unlock()
	}
	c.nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncNodesCache(timeout time.Duration) error {
	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.nodeInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindServiceInformer() {
	if !c.isFollowResource(TypeService) {
		return
	}

	c.serviceInformer = c.factory.Core().V1().Services().Informer()
	add := func(obj interface{}) {
		value, ok := obj.(*corev1.Service)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			data = newDataSet(value.Namespace)
			c.cache[value.Namespace] = data
		}

		data.upsertService(value)
	}

	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		value, ok := obj.(*corev1.Service)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			return
		}

		data.deleteService(value)
	}

	c.serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncServicesCache(timeout time.Duration) error {
	if !c.isFollowResource(TypeService) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.serviceInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindDeploymentInformer() {
	if !c.isFollowResource(TypeDeployment) {
		return
	}

	c.deploymentInformer = c.factory.Apps().V1().Deployments().Informer()
	add := func(obj interface{}) {
		value, ok := obj.(*v1.Deployment)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			data = newDataSet(value.Namespace)
			c.cache[value.Namespace] = data
		}

		data.upsertDeployment(value)
	}

	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		value, ok := obj.(*corev1.Service)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			return
		}
		data.deleteService(value)
	}

	c.deploymentInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncDeploymentsCache(timeout time.Duration) error {
	if !c.isFollowResource(TypeDeployment) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.deploymentInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindReplicaInformer() {
	if !c.isFollowResource(TypeReplicaSet) {
		return
	}

	c.replicaInformer = c.factory.Apps().V1().ReplicaSets().Informer()
	add := func(obj interface{}) {
		value, ok := obj.(*v1.ReplicaSet)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			data = newDataSet(value.Namespace)
			c.cache[value.Namespace] = data
		}

		data.upsertReplica(value)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		value, ok := obj.(*v1.ReplicaSet)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			return
		}
		data.deleteReplica(value)
	}

	c.replicaInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncReplicasCache(timeout time.Duration) error {
	if !c.isFollowResource(TypeReplicaSet) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.replicaInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindStatefulInformer() {
	if !c.isFollowResource(TypeStatefulSet) {
		return
	}

	c.statefulInformer = c.factory.Apps().V1().StatefulSets().Informer()
	add := func(obj interface{}) {
		value, ok := obj.(*v1.StatefulSet)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			data = newDataSet(value.Namespace)
			c.cache[value.Namespace] = data
		}

		data.upsertStateful(value)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		value, ok := obj.(*v1.StatefulSet)
		if !ok {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			return
		}
		data.deleteStateful(value)
	}

	c.statefulInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncStatefulCache(timeout time.Duration) error {
	if !c.isFollowResource(TypeStatefulSet) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.statefulInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindDaemonInformer() {
	if !c.isFollowResource(TypeDaemonSet) {
		return
	}

	c.daemonInformer = c.factory.Apps().V1().DaemonSets().Informer()
	add := func(obj interface{}) {
		value, ok := obj.(*v1.DaemonSet)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			data = newDataSet(value.Namespace)
			c.cache[value.Namespace] = data
		}

		data.upsertDaemon(value)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		value, ok := obj.(*v1.DaemonSet)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			return
		}
		data.deleteDaemon(value)
	}

	c.daemonInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncDaemonCache(timeout time.Duration) error {
	if !c.isFollowResource(TypeDaemonSet) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.daemonInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) bindEventInformer() {
	if !c.isFollowResource(TypePod) {
		return
	}

	c.eventInformer = c.factory.Core().V1().Events().Informer()
	add := func(obj interface{}) {
		value, ok := obj.(*corev1.Event)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			data = newDataSet(value.Namespace)
			c.cache[value.Namespace] = data
		}

		data.upsertEvent(value)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		value, ok := obj.(*corev1.Event)
		if !ok {
			return
		}
		if !c.isFollowNamespace(value.Namespace) {
			return
		}

		c.cacheMutex.Lock()
		defer c.cacheMutex.Unlock()

		data, ok := c.cache[value.Namespace]
		if !ok {
			return
		}
		data.deleteEvent(value)
	}

	c.eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncEventsCache(timeout time.Duration) error {
	if !c.isFollowResource(TypeEvent) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.eventInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) ListDeployments(ns string) (*v1.DeploymentList, error) {
	items, err := c.k8sClient.client.AppsV1().Deployments(ns).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchDeployments(ns string) (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.AppsV1().Deployments(ns).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListReplicaSets(ns string) (*v1.ReplicaSetList, error) {
	items, err := c.k8sClient.client.AppsV1().ReplicaSets(ns).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchReplicaSets(ns string) (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.AppsV1().ReplicaSets(ns).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListPods(ns string) (*corev1.PodList, error) {
	items, err := c.k8sClient.client.CoreV1().Pods(c.namespace).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchPods(ns string) (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.CoreV1().Pods(ns).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListServices(ns string) (*corev1.ServiceList, error) {
	items, err := c.k8sClient.client.CoreV1().Services(ns).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchServices(ns string) (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.CoreV1().Services(ns).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListEvents(ns string) (*corev1.EventList, error) {
	items, err := c.k8sClient.client.CoreV1().Events(ns).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchEvents(ns string) (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.CoreV1().Events(ns).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return watcher.ResultChan(), nil
}

func (c *ClusterCache) beforeValidate() error {
	if !c.isReady {
		return ErrCacheNotReady
	}
	return nil
}

func (c *ClusterCache) GetNodes() (map[string]*corev1.Node, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	ret := make(map[string]*corev1.Node, len(c.nodes))
	for name, node := range c.nodes {
		ret[name] = node
	}
	return ret, nil
}

func (c *ClusterCache) GetUpdateTime(ns string) (time.Time, error) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return time.Time{}, ErrNotFoundNamespace
	}

	return data.UpdateAT, nil
}

func (c *ClusterCache) GetNamespaces() (map[string]*corev1.Namespace, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	namespaces := make(map[string]*corev1.Namespace, len(c.namespaces))
	for name, obj := range c.namespaces {
		namespaces[name] = obj
	}
	return namespaces, nil
}

func (c *ClusterCache) GetPodsWithNS(ns string) (map[string]*corev1.Pod, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyPods(), nil
}

func (c *ClusterCache) GetPod(ns string, name string) (*corev1.Pod, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	pod, ok := data.Pods[name]
	if !ok {
		return nil, ErrNotFoundName
	}

	return pod, nil
}

func (c *ClusterCache) GetServices(ns string) (map[string]*corev1.Service, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyServices(), nil
}

func (c *ClusterCache) GetService(ns string, name string) (*corev1.Service, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	service, ok := data.Services[name]
	if !ok {
		return nil, ErrNotFoundName
	}

	return service, nil
}

func (c *ClusterCache) GetDeployments(ns string) (map[string]*v1.Deployment, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyDeployments(), nil
}

func (c *ClusterCache) GetDeployment(ns string, name string) (*v1.Deployment, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	dm, ok := data.Deployments[name]
	if !ok {
		return nil, ErrNotFoundName
	}

	return dm, nil
}

func (c *ClusterCache) GetReplicas(ns string) (map[string]*v1.ReplicaSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyReplicas(), nil
}

func (c *ClusterCache) GetReplica(ns string, name string) (*v1.ReplicaSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	rep, ok := data.Replicas[name]
	if !ok {
		return nil, ErrNotFoundName
	}

	return rep, nil
}

func (c *ClusterCache) GetDaemons(ns string) (map[string]*v1.DaemonSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyDaemons(), nil
}

func (c *ClusterCache) GetDaemon(ns string, name string) (*v1.DaemonSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	rep, ok := data.Daemons[name]
	if !ok {
		return nil, ErrNotFoundName
	}

	return rep, nil
}

func (c *ClusterCache) GetStatefuls(ns string) (map[string]*v1.StatefulSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyStatefuls(), nil
}

func (c *ClusterCache) GetStateful(ns string, name string) (*v1.StatefulSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	rep, ok := data.StatefulSets[name]
	if !ok {
		return nil, ErrNotFoundName
	}

	return rep, nil
}

func newDataSet(ns string) *DataSet {
	return &DataSet{
		Namespace:    ns,
		Pods:         make(map[string]*corev1.Pod, 10),
		Services:     make(map[string]*corev1.Service, 10),
		Deployments:  make(map[string]*v1.Deployment, 10),
		Replicas:     make(map[string]*v1.ReplicaSet, 10),
		Daemons:      make(map[string]*v1.DaemonSet, 10),
		StatefulSets: make(map[string]*v1.StatefulSet, 10),
		Events:       make(map[string][]*corev1.Event, 10),
	}
}

type DataSet struct {
	Namespace    string                     // current ns
	Pods         map[string]*corev1.Pod     // key: pod.name value: pod body
	Services     map[string]*corev1.Service // ...
	Deployments  map[string]*v1.Deployment  // ...
	Replicas     map[string]*v1.ReplicaSet  // ...
	Daemons      map[string]*v1.DaemonSet
	StatefulSets map[string]*v1.StatefulSet
	Events       map[string][]*corev1.Event
	UpdateAT     time.Time

	sync.RWMutex // todo
}

func (d *DataSet) upsertPod(pod *corev1.Pod) {
	d.Pods[pod.Name] = pod
	d.updateTime()
}

func (d *DataSet) deletePod(pod *corev1.Pod) {
	delete(d.Pods, pod.Name)
	d.updateTime()
}

func (d *DataSet) upsertService(data *corev1.Service) {
	d.Services[data.Name] = data
	d.updateTime()
}

func (d *DataSet) deleteService(data *corev1.Service) {
	delete(d.Services, data.Name)
	d.updateTime()
}

func (d *DataSet) upsertDeployment(data *v1.Deployment) {
	d.Deployments[data.Name] = data
	d.updateTime()
}

func (d *DataSet) deleteDeployment(data *v1.Deployment) {
	delete(d.Deployments, data.Name)
	d.updateTime()
}

func (d *DataSet) upsertReplica(data *v1.ReplicaSet) {
	d.Replicas[data.Name] = data
	d.updateTime()
}

func (d *DataSet) deleteReplica(data *v1.ReplicaSet) {
	delete(d.Replicas, data.Name)
	d.updateTime()
}

func (d *DataSet) upsertDaemon(data *v1.DaemonSet) {
	d.Daemons[data.Name] = data
	d.updateTime()
}

func (d *DataSet) deleteDaemon(data *v1.DaemonSet) {
	delete(d.Daemons, data.Name)
	d.updateTime()
}

func (d *DataSet) upsertStateful(data *v1.StatefulSet) {
	d.StatefulSets[data.Name] = data
	d.updateTime()
}

func (d *DataSet) deleteStateful(data *v1.StatefulSet) {
	delete(d.StatefulSets, data.Name)
	d.updateTime()
}

func (d *DataSet) upsertEvent(data *corev1.Event) {
	events, ok := d.Events[data.Name]
	if !ok {
		events = make([]*corev1.Event, 0, 5)
	}
	// reduce
	if len(events) > 50 {
		events = events[20:]
	}
	events = append(events, data)
	d.Events[data.Name] = events
	d.updateTime()
}

func (d *DataSet) deleteEvent(data *corev1.Event) {
	delete(d.Events, data.Name)
	d.updateTime()
}

func (d *DataSet) updateTime() {
	d.UpdateAT = time.Now()
}

func (d *DataSet) CopyPods() map[string]*corev1.Pod {
	resp := make(map[string]*corev1.Pod, len(d.Pods))
	for key, val := range d.Pods {
		resp[key] = val
	}
	return resp
}

func (d *DataSet) CopyServices() map[string]*corev1.Service {
	resp := make(map[string]*corev1.Service, len(d.Services))
	for key, val := range d.Services {
		resp[key] = val
	}
	return resp
}

func (d *DataSet) CopyReplicas() map[string]*v1.ReplicaSet {
	resp := make(map[string]*v1.ReplicaSet, len(d.Replicas))
	for key, val := range d.Replicas {
		resp[key] = val
	}
	return resp
}

func (d *DataSet) CopyDeployments() map[string]*v1.Deployment {
	resp := make(map[string]*v1.Deployment, len(d.Deployments))
	for key, val := range d.Deployments {
		resp[key] = val
	}
	return resp
}

func (d *DataSet) CopyStatefuls() map[string]*v1.StatefulSet {
	resp := make(map[string]*v1.StatefulSet, len(d.StatefulSets))
	for key, val := range d.StatefulSets {
		resp[key] = val
	}
	return resp
}

func (d *DataSet) CopyDaemons() map[string]*v1.DaemonSet {
	resp := make(map[string]*v1.DaemonSet, len(d.Daemons))
	for key, val := range d.Daemons {
		resp[key] = val
	}
	return resp
}

func int32Ptr2(i int32) *int32 {
	return &i
}

func getAllResources() map[string]bool {
	ret := map[string]bool{}
	for _, res := range allResources {
		ret[res] = true
	}
	return ret
}
