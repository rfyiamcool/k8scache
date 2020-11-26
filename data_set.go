package k8scache

import (
	"sync"
	"time"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func newDataSet(ns string) *DataSet {
	return &DataSet{
		Namespace:    ns,
		Pods:         make(map[string]*corev1.Pod, 10),
		Services:     make(map[string]*corev1.Service, 10),
		Deployments:  make(map[string]*v1.Deployment, 10),
		Replicas:     make(map[string]*v1.ReplicaSet, 10),
		Daemons:      make(map[string]*v1.DaemonSet, 10),
		StatefulSets: make(map[string]*v1.StatefulSet, 10),
		Events:       make(map[string]map[string][]*corev1.Event, 10),
		Endpoints:    make(map[string]*corev1.Endpoints, 10),
	}
}

type DataSet struct {
	Pods         map[string]*corev1.Pod // key: pod.name value: pod body
	Services     map[string]*corev1.Service
	Deployments  map[string]*v1.Deployment
	Replicas     map[string]*v1.ReplicaSet
	Daemons      map[string]*v1.DaemonSet
	StatefulSets map[string]*v1.StatefulSet
	Endpoints    map[string]*corev1.Endpoints
	Events       map[string]map[string][]*corev1.Event // key: Kind, key inside: name
	Namespace    string                                // current ns
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
	kind := data.InvolvedObject.Kind
	kinds, ok := d.Events[kind]
	if !ok {
		kinds = make(map[string][]*corev1.Event, 5)
	}

	events, ok := kinds[data.Name]
	if !ok {
		events = make([]*corev1.Event, 0, 5)
	}
	// reduce
	if len(events) > 50 {
		events = events[20:]
	}
	events = append(events, data)
	kinds[data.Name] = events
	d.Events[kind] = kinds
	d.updateTime()
}

func (d *DataSet) deleteEvent(data *corev1.Event) {
	delete(d.Events, data.Kind)
	d.updateTime()
}

func (d *DataSet) upsertEndpoint(data *corev1.Endpoints) {
	d.Endpoints[data.Name] = data
	d.updateTime()
}

func (d *DataSet) deleteEndpoint(data *corev1.Endpoints) {
	delete(d.Endpoints, data.Name)
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

func (d *DataSet) CopyEvents() map[string]map[string][]*corev1.Event {
	resp := make(map[string]map[string][]*corev1.Event, len(d.Events))
	for kind, names := range d.Events {
		resp[kind] = make(map[string][]*corev1.Event, len(names))
		for name, evs := range names {
			resp[kind][name] = evs
		}
	}
	return resp
}

func (d *DataSet) CopyEndpoints() map[string]*corev1.Endpoints {
	resp := make(map[string]*corev1.Endpoints, len(d.Daemons))
	for key, val := range d.Endpoints {
		resp[key] = val
	}
	return resp
}
