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
	TypePod         = "pod"
	TypeNode        = "node"
	TypeService     = "Service"
	TypeReplicaSet  = "ReplicaSet"
	TypeDeployment  = "Deployment"
	TypeDaemonSet   = "DaemonSet"
	TypeStatefulSet = "StatefulSet"
)

var (
	ErrNotFoundNamespace   = errors.New("not found namespace")
	ErrNotFoundName        = errors.New("not found name")
	ErrSyncResourceTimeout = errors.New("sync resource timeout")

	allResources = map[string]bool{
		TypePod:         true,
		TypeNode:        true,
		TypeService:     true,
		TypeReplicaSet:  true,
		TypeDeployment:  true,
		TypeDaemonSet:   true,
		TypeStatefulSet: true,
	}
)

type Client struct {
	config *rest.Config
	client *kubernetes.Clientset
}

func NewClient() (*Client, error) {
	return &Client{}, nil
}

func (cs *Client) GetConfig() *rest.Config {
	return cs.config
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
	var err error
	cs.config, err = clientcmd.BuildConfigFromFlags("", kubecfg)
	if err != nil {
		return err
	}

	cs.client, err = kubernetes.NewForConfig(cs.config)
	return err
}

func (cs *Client) BuildForInCluster(kubecfg string) error {
	var err error
	cs.config, err = rest.InClusterConfig()
	if err != nil {
		return err
	}

	cs.client, err = kubernetes.NewForConfig(cs.config)
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

	cs.config = cfg
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

func NewClusterCache(client *Client, opts ...OptionFunc) (*ClusterCache, error) {
	cc := &ClusterCache{
		k8sClient:      client,
		stopper:        make(chan struct{}, 0),
		cache:          make(map[string]*DataSet, 5),
		nodes:          make(map[string]*corev1.Node, 5),
		followResource: allResources,
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
	ctx            context.Context
	cancel         context.CancelFunc
	namespace      string
	k8sClient      *Client
	followResource map[string]bool

	stopper  chan struct{}
	stopOnce sync.Once

	factory            informers.SharedInformerFactory
	podInformer        cache.SharedIndexInformer
	nodeInformer       cache.SharedIndexInformer
	serviceInformer    cache.SharedIndexInformer
	deploymentInformer cache.SharedIndexInformer
	replicaInformer    cache.SharedIndexInformer
	daemonInformer     cache.SharedIndexInformer
	statefulInformer   cache.SharedIndexInformer

	nodes      map[string]*corev1.Node
	cache      map[string]*DataSet
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
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}

func (c *ClusterCache) isFollowResource(res string) bool {
	_, ok := c.followResource[res]
	return ok
}

func (c *ClusterCache) bindResourceInformer() {
	c.bindPodInformer()
	c.bindNodesInformer()
	c.bindReplicaInformer()
	c.bindServiceInformer()
	c.bindDeploymentInformer()
	c.bindStatefulInformer()
	c.bindDaemonInformer()
}

var (
	defaultSyncTimeout = time.Duration(30 * time.Second)
)

func (c *ClusterCache) SyncCache() error {
	return c.SyncCacheWithTimeout(defaultSyncTimeout)
}

func (c *ClusterCache) TimeoutChan(ts time.Duration) (chan struct{}, *time.Timer) {
	done := make(chan struct{})
	if ts == 0 {
		return done, time.NewTimer(0)
	}

	timer := time.AfterFunc(ts, func() {
		close(done)
	})
	return done, timer
}

func (c *ClusterCache) SyncCacheWithTimeout(timeout time.Duration) error {
	var (
		funcs = []func(time.Duration) error{}
		err   error
	)

	funcs = append(funcs,
		c.SyncPodsCache,
		c.SyncNodesCache,
		c.SyncServicesCache,
		c.SyncDeploymentsCache,
		c.SyncReplicasCache,
		c.SyncDaemonCache,
		c.SyncStatefulCache,
	)

	wg := sync.WaitGroup{}
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

	return err
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

	done, timer := c.TimeoutChan(timeout)
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
	done, timer := c.TimeoutChan(timeout)
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

	done, timer := c.TimeoutChan(timeout)
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

	done, timer := c.TimeoutChan(timeout)
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

	done, timer := c.TimeoutChan(timeout)
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

	done, timer := c.TimeoutChan(timeout)
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

	done, timer := c.TimeoutChan(timeout)
	defer timer.Stop()

	if !cache.WaitForCacheSync(done, c.daemonInformer.HasSynced) {
		return ErrSyncResourceTimeout
	}
	return nil
}

func (c *ClusterCache) ListDeployments() (*v1.DeploymentList, error) {
	items, err := c.k8sClient.client.AppsV1().Deployments(c.namespace).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchDeployments() (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.AppsV1().Deployments(c.namespace).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListReplicaSets() (*v1.ReplicaSetList, error) {
	items, err := c.k8sClient.client.AppsV1().ReplicaSets(metav1.NamespaceDefault).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchReplicaSets() (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.AppsV1().ReplicaSets(c.namespace).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListPods() (*corev1.PodList, error) {
	items, err := c.k8sClient.client.CoreV1().Pods(c.namespace).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchPods() (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.CoreV1().Pods(c.namespace).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return watcher.ResultChan(), nil
}

func (c *ClusterCache) ListServices() (*corev1.ServiceList, error) {
	items, err := c.k8sClient.client.CoreV1().Services(c.namespace).List(metav1.ListOptions{})
	return items, err
}

func (c *ClusterCache) WatchServices() (<-chan watch.Event, error) {
	watcher, err := c.k8sClient.client.CoreV1().Services(c.namespace).Watch(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return watcher.ResultChan(), nil
}

func (c *ClusterCache) GetNodes() (map[string]*corev1.Node, error) {
	ret := make(map[string]*corev1.Node, len(c.nodes))
	for name, node := range c.nodes {
		ret[name] = node
	}
	return ret, nil
}

func (c *ClusterCache) GetUpdateTimeWithNs(ns string) (time.Time, error) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return time.Time{}, ErrNotFoundNamespace
	}

	return data.UpdateAT, nil
}

func (c *ClusterCache) GetNamespaces() ([]string, error) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	namespaces := []string{}
	for ns, _ := range c.cache {
		namespaces = append(namespaces, ns)
	}
	return namespaces, nil
}

func (c *ClusterCache) GetPodsWithNS(ns string) (map[string]*corev1.Pod, error) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyPods(), nil
}

func (c *ClusterCache) GetPodWithNS(ns string, name string) (*corev1.Pod, error) {
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

func (c *ClusterCache) GetServicesWithNS(ns string) (map[string]*corev1.Service, error) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyServices(), nil
}

func (c *ClusterCache) GetServiceWithNS(ns string, name string) (*corev1.Service, error) {
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

func (c *ClusterCache) GetDeploymentsWithNS(ns string) (map[string]*v1.Deployment, error) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyDeployments(), nil
}

func (c *ClusterCache) GetDeploymentWithNS(ns string, name string) (*v1.Deployment, error) {
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

func (c *ClusterCache) GetReplicasWithNS(ns string) (map[string]*v1.ReplicaSet, error) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	data, ok := c.cache[ns]
	if !ok {
		return nil, ErrNotFoundNamespace
	}

	// copy
	return data.CopyReplicas(), nil
}

func (c *ClusterCache) GetReplicaWithNS(ns string, name string) (*v1.ReplicaSet, error) {
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

func newDataSet(ns string) *DataSet {
	return &DataSet{
		Namespace:   ns,
		Pods:        make(map[string]*corev1.Pod, 10),
		Services:    make(map[string]*corev1.Service, 10),
		Deployments: make(map[string]*v1.Deployment, 10),
		Replicas:    make(map[string]*v1.ReplicaSet, 10),
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

func int32Ptr2(i int32) *int32 {
	return &i
}
