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
	listappv1 "k8s.io/client-go/listers/apps/v1"
	listcorev1 "k8s.io/client-go/listers/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	TypeEndpoint    = "Endpoint"
)

var (
	ErrNotFoundNamespace    = errors.New("not found namespace")
	ErrNotFoundName         = errors.New("not found name")
	ErrSyncResourceTimeout  = errors.New("sync resource timeout")
	ErrCacheNotReady        = errors.New("cache not ready, wait to sync full cache in the beginning")
	ErrBeyondExpireInterval = errors.New("beyond expire interval")
	ErrNotSupportResource   = errors.New("not support resource")

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
		TypeEndpoint,
	}
)

type Client struct {
	client *kubernetes.Clientset
}

func NewClient() (*Client, error) {
	return &Client{}, nil
}

func (cs *Client) SetClient(cli *kubernetes.Clientset) {
	cs.client = cli
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
		listeners:        newListenerPool(),
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
	isExpired        bool
	listeners        *ListenerPool

	stopper  chan struct{}
	stopOnce sync.Once

	factory            informers.SharedInformerFactory
	namespaceInformer  cache.SharedIndexInformer
	namespaceLister    listcorev1.NamespaceLister
	podInformer        cache.SharedIndexInformer
	podLister          listcorev1.PodLister
	nodeInformer       cache.SharedIndexInformer
	nodeLister         listcorev1.NodeLister
	serviceInformer    cache.SharedIndexInformer
	serviceLister      listcorev1.ServiceLister
	deploymentInformer cache.SharedIndexInformer
	deploymentLister   listappv1.DeploymentLister
	replicaInformer    cache.SharedIndexInformer
	replicaLister      listappv1.ReplicaSetLister
	daemonInformer     cache.SharedIndexInformer
	daemonLister       listappv1.DaemonSetLister
	statefulInformer   cache.SharedIndexInformer
	statefulLister     listappv1.StatefulSetLister
	eventInformer      cache.SharedIndexInformer
	eventLister        listcorev1.EventLister
	endpointInformer   cache.SharedIndexInformer
	endpointLister     listcorev1.EndpointsLister

	nodes      map[string]*corev1.Node
	namespaces map[string]*corev1.Namespace
	cache      map[string]*DataSet // key: namespace, val: *DataSet
	cacheMutex sync.RWMutex
}

func (c *ClusterCache) Start() {
	c.factory = informers.NewSharedInformerFactory(c.k8sClient.client, 0)
	c.bindResourceInformer()
	c.factory.Start(c.stopper)
	go c.healthScaner()
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

func (c *ClusterCache) RegisterListener(genre string, lis Listener) {
	c.listeners.add(genre, lis)
}

func (c *ClusterCache) UnRegisterListener(genre string, lis Listener) {
	c.listeners.del(genre, lis)
}

func (c *ClusterCache) healthScaner() {
	for {
		select {
		case <-c.stopper:
			return
		case <-time.After(5 * time.Second):
		}

		_, err := c.k8sClient.client.CoreV1().Namespaces().Get(metav1.NamespaceDefault, metav1.GetOptions{})
		if err != nil {
			c.isExpired = true
		}
	}
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
	c.bindEndpointsInformer()
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
		c.SyncEndpointsCache,
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

func (c *ClusterCache) Labels(mm map[string]string) labels.Selector {
	return labels.Set(mm).AsSelector()
}

func (c *ClusterCache) bindNamespaceInformer() {
	c.namespaceInformer = c.factory.Core().V1().Namespaces().Informer()
	c.namespaceLister = c.factory.Core().V1().Namespaces().Lister()
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
	c.podLister = c.factory.Core().V1().Pods().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypePod, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypePod, obj)
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
	c.nodeLister = c.factory.Core().V1().Nodes().Lister()

	c.nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    nil,
		UpdateFunc: nil,
		DeleteFunc: nil,
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
	c.serviceLister = c.factory.Core().V1().Services().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypeService, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypeService, obj)
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
	c.deploymentLister = c.factory.Apps().V1().Deployments().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypeDeployment, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypeDeployment, obj)
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
	c.replicaLister = c.factory.Apps().V1().ReplicaSets().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypeReplicaSet, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypeReplicaSet, obj)
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
	c.statefulLister = c.factory.Apps().V1().StatefulSets().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypeStatefulSet, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypeStatefulSet, obj)
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
	c.daemonLister = c.factory.Apps().V1().DaemonSets().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypeDaemonSet, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypeDaemonSet, obj)
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
	c.eventLister = c.factory.Core().V1().Events().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypeEvent, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypeEvent, obj)
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

func (c *ClusterCache) bindEndpointsInformer() {
	if !c.isFollowResource(TypeEndpoint) {
		return
	}

	c.endpointInformer = c.factory.Core().V1().Endpoints().Informer()
	c.endpointLister = c.factory.Core().V1().Endpoints().Lister()

	add := func(obj interface{}) {
		c.listeners.xrange(TypeEndpoint, obj)
	}
	update := func(oldObj, newObj interface{}) {
		add(newObj)
	}
	del := func(obj interface{}) {
		c.listeners.xrange(TypeEndpoint, obj)
	}

	c.endpointInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    add,
		UpdateFunc: update,
		DeleteFunc: del,
	})
}

func (c *ClusterCache) SyncEndpointsCache(timeout time.Duration) error {
	if !c.isFollowResource(TypeEndpoint) {
		return nil
	}

	done, timer := c.timeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.endpointInformer.HasSynced) {
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

func (c *ClusterCache) ListStatefuls(ns string) (*v1.StatefulSetList, error) {
	items, err := c.k8sClient.client.AppsV1().StatefulSets(ns).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchStatefuls(ns string) (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.AppsV1().StatefulSets(ns).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListDaemons(ns string) (*v1.DaemonSetList, error) {
	items, err := c.k8sClient.client.AppsV1().DaemonSets(ns).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchDaemons(ns string) (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.AppsV1().DaemonSets(ns).Watch(metav1.ListOptions{})
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
	if c.isExpired {
		return ErrBeyondExpireInterval
	}
	return nil
}

func (c *ClusterCache) GetNodes() ([]*corev1.Node, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.nodeLister.List(labels.Everything())
}

func (c *ClusterCache) GetUpdateTime(ns string) (time.Time, error) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	data, ok := c.cache[ns]
	if !ok {
		return time.Time{}, ErrNotFoundNamespace
	}

	return data.UpdateAT, nil
}

func (c *ClusterCache) GetNamespaces() ([]*corev1.Namespace, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.namespaceLister.List(labels.Everything())
}

func (c *ClusterCache) GetAllPods(ns string) ([]*corev1.Pod, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.podLister.Pods(ns).List(labels.Everything())
}

func (c *ClusterCache) GetPod(ns string, name string) (*corev1.Pod, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.podLister.Pods(ns).Get(name)
}

func (c *ClusterCache) GetAllServices(ns string) ([]*corev1.Service, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.serviceLister.Services(ns).List(labels.Everything())
}

func (c *ClusterCache) GetService(ns string, name string) (*corev1.Service, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.serviceLister.Services(ns).Get(name)
}

func (c *ClusterCache) GetAllDeployments(ns string) ([]*v1.Deployment, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.deploymentLister.Deployments(ns).List(labels.Everything())
}

func (c *ClusterCache) GetDeployment(ns string, name string) (*v1.Deployment, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.deploymentLister.Deployments(ns).Get(name)
}

func (c *ClusterCache) GetAllReplicas(ns string) ([]*v1.ReplicaSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.replicaLister.ReplicaSets(ns).List(labels.Everything())
}

func (c *ClusterCache) GetReplica(ns string, name string) (*v1.ReplicaSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.replicaLister.ReplicaSets(ns).Get(name)
}

func (c *ClusterCache) GetAllDaemons(ns string) ([]*v1.DaemonSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.daemonLister.DaemonSets(ns).List(labels.Everything())
}

func (c *ClusterCache) GetAllDaemonsWithLabels(ns string, filter labels.Selector) ([]*v1.DaemonSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.daemonLister.DaemonSets(ns).List(filter)
}

func (c *ClusterCache) GetDaemon(ns string, name string) (*v1.DaemonSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.daemonLister.DaemonSets(ns).Get(name)
}

func (c *ClusterCache) GetAllStatefuls(ns string) ([]*v1.StatefulSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.statefulLister.StatefulSets(ns).List(labels.Everything())
}

func (c *ClusterCache) GetAllStatefulsWithLabels(ns string, filter labels.Selector) ([]*v1.StatefulSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.statefulLister.StatefulSets(ns).List(nil)
}

func (c *ClusterCache) GetStateful(ns string, name string) (*v1.StatefulSet, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.statefulLister.StatefulSets(ns).Get(name)
}

func (c *ClusterCache) GetAllEvents(ns string) ([]*corev1.Event, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.eventLister.Events(ns).List(labels.Everything())
}

func (c *ClusterCache) GetAllEventsWithLabels(ns string, filter labels.Selector) ([]*corev1.Event, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.eventLister.Events(ns).List(filter)
}

func (c *ClusterCache) GetEvents(ns, name string) (*corev1.Event, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.eventLister.Events(ns).Get(name)
}

func (c *ClusterCache) GetAllEndpoints(ns string) ([]*corev1.Endpoints, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.endpointLister.Endpoints(ns).List(labels.Everything())
}

func (c *ClusterCache) GetAllEndpointsWithLabels(ns string, filter labels.Selector) ([]*corev1.Endpoints, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.endpointLister.Endpoints(ns).List(filter)
}

func (c *ClusterCache) GetEndpoints(ns string, name string) (*corev1.Endpoints, error) {
	if err := c.beforeValidate(); err != nil {
		return nil, err
	}

	return c.endpointLister.Endpoints(ns).Get(name)
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

func isSupportedResource(genre string) bool {
	for _, res := range allResources {
		if res == genre {
			return true
		}
	}
	return false
}
