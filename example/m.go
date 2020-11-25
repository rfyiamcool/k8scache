package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err)
	}

	// 初始化 client
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Panic(err.Error())
	}

	stopper := make(chan struct{})
	// defer close(stopper)

	time.AfterFunc(60*time.Second, func() {
		close(stopper)
	})

	// 初始化 informer
	factory := informers.NewSharedInformerFactory(clientset, 0)

	// defer runtime.HandleCrash()

	podFactory := factory.Core().V1().Pods()
	podInformer := podFactory.Informer()

	nodeFactory := factory.Core().V1().Nodes()
	nodeInformer := nodeFactory.Informer()

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAdd,
		UpdateFunc: onUpdate,
		DeleteFunc: onDelete,
	})

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: onNodeAdd,
	})

	factory.Start(stopper)

	if !cache.WaitForCacheSync(stopper, podInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}
	if !cache.WaitForCacheSync(stopper, nodeInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	select {
	case <-stopper:
	}
}

func onNodeAdd(obj interface{}) {
	fmt.Printf("add node type %v", reflect.TypeOf(obj))
	node := obj.(*corev1.Node)
	fmt.Println("add node:", node.Name)
}

func onAdd(obj interface{}) {
	fmt.Printf("add pod type %v", reflect.TypeOf(obj))
	pod := obj.(*corev1.Pod)
	fmt.Println("add pod:", pod.Name)
}

func onUpdate(old, cur interface{}) {
	fmt.Printf("update pod %v %v \n", reflect.TypeOf(old), reflect.TypeOf(cur))
}

func onDelete(obj interface{}) {
	fmt.Println("del pod", obj)
}
