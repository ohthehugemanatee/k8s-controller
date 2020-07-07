package main

// Kubernetes Controller which demonstrates multiple "state gates".

import (
	"fmt"
	"strings"
	"time"

	"github.com/kubernetes/client-go/tools/cache"
	"github.com/kubernetes/client-go/util/workqueue"
	log "github.com/sirupsen/logrus"
	apps_v1 "k8s.io/api/apps/v1"
	batch_v1 "k8s.io/api/batch/v1"
	api_v1 "k8s.io/api/core/v1"
	ext_v1beta1 "k8s.io/api/extensions/v1beta1"
	rbac_v1beta1 "k8s.io/api/rbac/v1beta1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

var serverStartTime time.Time

const maxRetries = 5

// Event object.
type k8sEvent struct {
	Namespace string
	Kind      string
	Component string
	Host      string
	Reason    string
	Status    string
	Name      string
}

// Event indicate the informerEvent
type event struct {
	key          string
	eventType    string
	namespace    string
	resourceType string
}

// Handler processes an event.
type handler interface {
	Handle(e k8sEvent)
}

// Controller object.
type Controller struct {
	logger       *log.Entry
	clientset    kubernetes.Interface
	queue        workqueue.RateLimitingInterface
	informer     cache.SharedIndexInformer
	eventHandler handler
}

func main() {
	// Instantiate the queue and informer.
	var kubeClient kubernetes.Interface
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
				return kubeClient.AppsV1().Deployments(meta_v1.NamespaceAll).List(options)
			},
			WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
				return kubeClient.AppsV1().Deployments(meta_v1.NamespaceAll).Watch(options)
			},
		},
		&api_v1.Pod{},
		0, // No resync
		cache.Indexers{},
	)
	// Add an event Handler to the informer.
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	})
}

// Run starts the controller.
func (c *Controller) Run(stopCh <-chan struct{}) {
	// Don't crash on panic.
	defer utilruntime.HandleCrash()
	// Ensure existing workers are exited before we start.
	defer c.queue.ShutDown()

	c.logger.Info("Starting custom controller")

	go c.informer.Run(stopCh)
	// Sync caches before starting.
	if !cache.WaitForCacheSync(stopCh, c.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	c.logger.Info("Custom controller synced and ready")

	// runWorker is an infinite loop. If anything comes up in stopCh it will be killed after 1 second.
	wait.Until(c.runWorker, time.Second, stopCh)
}

// HasSynced is required for the cache.Controller interface.
func (c *Controller) HasSynced() bool {
	return c.informer.HasSynced()
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
		// loop forever.
	}
}

// Pulls a key off the top of the queue, processes it and either requeues or marks as done.
func (c *Controller) processNextItem() bool {
	newEvent, quit := c.queue.Get()

	if quit {
		return false
	}
	defer c.queue.Done(newEvent)
	// Actually process the item. This is where the magic happens.
	err := c.processItem(newEvent.(event))
	if err == nil {
		// No error, reset the NumRequeues counter.
		c.queue.Forget(newEvent)
	} else if c.queue.NumRequeues(newEvent) < maxRetries {
		c.logger.Errorf("Error processing %s (will retry): %v", newEvent.(event).key, err)
		c.queue.AddRateLimited(newEvent)
	} else {
		// No error but too many retries
		c.logger.Errorf("Error processing %s (giving up): %v", newEvent.(event).key, err)
		c.queue.Forget(newEvent)
		utilruntime.HandleError(err)
	}
	return true
}

// This is where the magic happens.
func (c *Controller) processItem(newEvent event) error {
	obj, _, err := c.informer.GetIndexer().GetByKey(newEvent.key)
	if err != nil {
		return fmt.Errorf("Error fetching object with key %s from store: %v", newEvent.key, err)
	}

	// get object's metedata
	objectMeta := getObjectMetaData(obj)

	// hold status type for default critical alerts
	var status string

	// namespace retrived from event key incase namespace value is empty
	if newEvent.namespace == "" && strings.Contains(newEvent.key, "/") {
		substring := strings.Split(newEvent.key, "/")
		newEvent.namespace = substring[0]
		newEvent.key = substring[1]
	}

	// process events based on its type
	switch newEvent.eventType {
	case "create":
		// compare CreationTimestamp and serverStartTime and alert only on latest events
		// Could be Replaced by using Delta or DeltaFIFO
		if objectMeta.CreationTimestamp.Sub(serverStartTime).Seconds() > 0 {
			switch newEvent.resourceType {
			case "NodeNotReady":
				status = "Danger"
			case "NodeReady":
				status = "Normal"
			case "NodeRebooted":
				status = "Danger"
			case "Backoff":
				status = "Danger"
			default:
				status = "Normal"
			}
			kbEvent := k8sEvent{
				Name:      objectMeta.Name,
				Namespace: newEvent.namespace,
				Kind:      newEvent.resourceType,
				Status:    status,
				Reason:    "Created",
			}
			c.eventHandler.Handle(kbEvent)
			return nil
		}
	case "update":
		/* TODOs
		- enahace update event processing in such a way that, it send alerts about what got changed.
		*/
		switch newEvent.resourceType {
		case "Backoff":
			status = "Danger"
		default:
			status = "Warning"
		}
		kbEvent := k8sEvent{
			Name:      newEvent.key,
			Namespace: newEvent.namespace,
			Kind:      newEvent.resourceType,
			Status:    status,
			Reason:    "Updated",
		}
		c.eventHandler.Handle(kbEvent)
		return nil
	case "delete":
		kbEvent := k8sEvent{
			Name:      newEvent.key,
			Namespace: newEvent.namespace,
			Kind:      newEvent.resourceType,
			Status:    "Danger",
			Reason:    "Deleted",
		}
		c.eventHandler.Handle(kbEvent)
		return nil
	}
	return nil
}

// GetObjectMetaData returns metadata of a given k8s object
func getObjectMetaData(obj interface{}) (objectMeta meta_v1.ObjectMeta) {

	switch object := obj.(type) {
	case *apps_v1.Deployment:
		objectMeta = object.ObjectMeta
	case *api_v1.ReplicationController:
		objectMeta = object.ObjectMeta
	case *apps_v1.ReplicaSet:
		objectMeta = object.ObjectMeta
	case *apps_v1.DaemonSet:
		objectMeta = object.ObjectMeta
	case *api_v1.Service:
		objectMeta = object.ObjectMeta
	case *api_v1.Pod:
		objectMeta = object.ObjectMeta
	case *batch_v1.Job:
		objectMeta = object.ObjectMeta
	case *api_v1.PersistentVolume:
		objectMeta = object.ObjectMeta
	case *api_v1.Namespace:
		objectMeta = object.ObjectMeta
	case *api_v1.Secret:
		objectMeta = object.ObjectMeta
	case *ext_v1beta1.Ingress:
		objectMeta = object.ObjectMeta
	case *api_v1.Node:
		objectMeta = object.ObjectMeta
	case *rbac_v1beta1.ClusterRole:
		objectMeta = object.ObjectMeta
	case *api_v1.ServiceAccount:
		objectMeta = object.ObjectMeta
	case *api_v1.Event:
		objectMeta = object.ObjectMeta
	}
	return objectMeta
}
