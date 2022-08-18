package retry

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	cache "k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"

	factory "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
)

const RetryObjInterval = 30 * time.Second
const MaxFailedAttempts = 15 // same value used for the services level-driven controller
const initialBackoff = 1
const noBackoff = 0

// retryObjEntry is a generic object caching with retry mechanism
// that resources can use to eventually complete their intended operations.
type retryObjEntry struct {
	// newObj holds k8s resource failed during add operation
	newObj interface{}
	// oldObj holds k8s resource failed during delete operation
	oldObj interface{}
	// config holds feature specific configuration,
	// currently used by network policies and pods.
	config     interface{}
	timeStamp  time.Time
	backoffSec time.Duration
	// number of times this object has been unsuccessfully added/updated/deleted
	failedAttempts uint8
}

type RetryFramework struct {
	// cache to hold object needs retry to successfully complete processing
	retryEntries *syncmap.SyncMap[*retryObjEntry]
	// resource type for these objects
	ObjType reflect.Type
	// channel to indicate we need to retry objs immediately
	retryChan chan struct{}
	// namespace filter fed to the handler for this resource type
	namespaceForFilteredHandler string
	// label selector fed to the handler for this resource type
	labelSelectorForFilteredHandler labels.Selector
	// sync function for the handler
	syncFunc func([]interface{}) error
	// extra parameters needed by specific types, for now
	// in use by network policy dynamic handlers// TODO this is directly assigned now, so remove this
	ExtraParameters interface{}

	watchFactory  *factory.WatchFactory
	StopChan      <-chan struct{} // TODO not sure how a channel shared with other controller works...
	HasUpdateFunc bool

	// Functions related to adding, updating, deleting and (initially) syncing a resource.
	// They will be initialized as no-op functions, then each client (ovnk master, ovnk node)
	// will have to override them with resource-specific code (e.g. for pods, nodes, etc.)
	AddResource    func(rf *RetryFramework, obj interface{}, fromRetryLoop bool) error           // TODO rf is useful only for ExtraParameters, which is private now
	UpdateResource func(rf *RetryFramework, oldObj, newObj interface{}, inRetryCache bool) error // TODO rf is useful only for ExtraParameters
	DeleteResource func(rf *RetryFramework, obj, cachedObj interface{}) error
	SyncFunc       func([]interface{}) error

	// auxiliary functions needed in the retry logic, initialized with a default behaviour
	// and to be overriden to implement resource-specific actions.
	GetResourceFromInformerCache func(key string) (interface{}, error)
	AreResourcesEqual            func(obj1, obj2 interface{}) (bool, error)
	GetInternalCacheEntry        func(obj interface{}) interface{}
	IsResourceScheduled          func(obj interface{}) bool
	ResourceNeedsUpdate          func() bool
	IsObjectInTerminalState      func(obj interface{}) bool

	// functions related to metrics and events
	RecordAddEvent     func(obj interface{})
	RecordUpdateEvent  func(obj interface{})
	RecordDeleteEvent  func(obj interface{})
	RecordSuccessEvent func(obj interface{})
	RecordErrorEvent   func(obj interface{}, err error)
}

// NewRetryFramework returns a new RetryFramework instance, essential for the whole retry logic.
// it returns a new struct packed with the desired input parameters and
// with its function attributes pre-filled with default code. Clients of this pkg (ovnk master,
// ovnk node) will have to override the functions in the returned struct with the desired
// per-resource logic.
func NewRetryFramework(
	objectType reflect.Type,
	namespaceForFilteredHandler string,
	labelSelectorForFilteredHandler labels.Selector,
	syncFunc func([]interface{}) error,
	extraParameters interface{},
	stopChan <-chan struct{},
	watchFactory *factory.WatchFactory) *RetryFramework {
	klog.Infof("NewRetryFramework for %s", objectType) // TODO remove this
	return &RetryFramework{
		retryEntries:                    syncmap.NewSyncMap[*retryObjEntry](),
		retryChan:                       make(chan struct{}, 1),
		ObjType:                         objectType,
		namespaceForFilteredHandler:     namespaceForFilteredHandler,
		labelSelectorForFilteredHandler: labelSelectorForFilteredHandler,
		syncFunc:                        syncFunc,
		ExtraParameters:                 extraParameters,

		watchFactory:  watchFactory,
		StopChan:      stopChan,
		HasUpdateFunc: false,

		// These functions should be overwritten by ovn-k master and node with
		// resource-specific code (e.g. pods, nodes, etc.)
		AddResource: func(rf *RetryFramework, obj interface{}, fromRetryLoop bool) error {
			klog.V(5).Infof("AddResource undefined for %s", objectType)
			return nil
		},
		UpdateResource: func(rf *RetryFramework, oldObj, newObj interface{}, inRetryCache bool) error {
			klog.V(5).Infof("UpdateResource undefined for %s", objectType)
			return nil
		},
		DeleteResource: func(rf *RetryFramework, obj interface{}, cachedObj interface{}) error {
			klog.V(5).Infof("DeleteResource undefined for %s", objectType)
			return nil
		},
		SyncFunc: func([]interface{}) error {
			klog.V(5).Infof("SyncFunc undefined for %s", objectType)
			return nil
		},

		GetResourceFromInformerCache: func(key string) (interface{}, error) {
			return nil, fmt.Errorf("GetResourceFromInformerCache undefined for %s", objectType)
		},

		AreResourcesEqual: func(obj1, obj2 interface{}) (bool, error) {
			return false, fmt.Errorf("AreResourcesEqual undefined for %s", objectType)
		},

		GetInternalCacheEntry: func(obj interface{}) interface{} { return nil },

		IsResourceScheduled: func(obj interface{}) bool { return true },

		ResourceNeedsUpdate: func() bool { return false },

		IsObjectInTerminalState: func(obj interface{}) bool { return false },

		RecordAddEvent:     func(obj interface{}) {},
		RecordUpdateEvent:  func(obj interface{}) {},
		RecordDeleteEvent:  func(obj interface{}) {},
		RecordSuccessEvent: func(obj interface{}) {},
		RecordErrorEvent:   func(obj interface{}, err error) {},
	}
}

func (rf *RetryFramework) DoWithLock(key string, f func(key string)) {
	rf.retryEntries.LockKey(key)
	defer rf.retryEntries.UnlockKey(key)
	f(key)
}

func (rf *RetryFramework) initRetryObjWithAddBackoff(obj interface{}, lockedKey string, backoff time.Duration) *retryObjEntry {
	// even if the object was loaded and changed before with the same lock, LoadOrStore will return reference to the same object
	entry := rf.retryEntries.LoadOrStore(lockedKey, &retryObjEntry{backoffSec: backoff})
	entry.timeStamp = time.Now()
	entry.newObj = obj
	entry.failedAttempts = 0
	entry.backoffSec = backoff
	return entry
}

// initRetryObjWithAdd creates a retry entry for an object that is being added,
// so that, if it fails, the add can be potentially retried later.
func (rf *RetryFramework) initRetryObjWithAdd(obj interface{}, lockedKey string) *retryObjEntry {
	return rf.initRetryObjWithAddBackoff(obj, lockedKey, initialBackoff)
}

// initRetryObjWithUpdate tracks objects that failed to be updated to potentially retry later
func (rf *RetryFramework) initRetryObjWithUpdate(oldObj, newObj interface{}, lockedKey string) *retryObjEntry {
	entry := rf.retryEntries.LoadOrStore(lockedKey, &retryObjEntry{config: oldObj, backoffSec: initialBackoff})
	// even if the object was loaded and changed before with the same lock, LoadOrStore will return reference to the same object
	entry.timeStamp = time.Now()
	entry.newObj = newObj
	entry.config = oldObj
	entry.failedAttempts = 0
	return entry
}

func A() {
}

// InitRetryObjWithDelete creates a retry entry for an object that is being deleted,
// so that, if it fails, the delete can be potentially retried later.
// When applied to pods, we include the config object as well in case the namespace is removed
// and the object is orphaned from the namespace. Similarly, when applied to network policies,
// we include in config the networkPolicy struct used internally, for the same scenario where
// a namespace is being deleted along with its network policies and, in case of a delete retry of
// one such network policy, we wouldn't be able to get to the networkPolicy struct from nsInfo.
//
// The noRetryAdd boolean argument is to indicate whether to retry for addition
func (rf *RetryFramework) InitRetryObjWithDelete(obj interface{}, lockedKey string, config interface{}, noRetryAdd bool) *retryObjEntry {
	// even if the object was loaded and changed before with the same lock, LoadOrStore will return reference to the same object
	entry := rf.retryEntries.LoadOrStore(lockedKey, &retryObjEntry{config: config, backoffSec: initialBackoff})
	entry.timeStamp = time.Now()
	entry.oldObj = obj
	if entry.config == nil {
		entry.config = config
	}
	entry.failedAttempts = 0
	if noRetryAdd {
		// will not be retried for addition
		entry.newObj = nil
	}
	return entry
}

// AddRetryObjWithAddNoBackoff adds an object to be retried immediately for add.
// It will lock the key, create or update retryObject, and unlock the key
func (rf *RetryFramework) AddRetryObjWithAddNoBackoff(obj interface{}) error {
	key, err := GetResourceKey(rf.ObjType, obj)
	if err != nil {
		return fmt.Errorf("could not get the key of %s %v: %v", rf.ObjType, obj, err)
	}
	rf.DoWithLock(key, func(key string) {
		rf.initRetryObjWithAddBackoff(obj, key, noBackoff)
	})
	return nil
}

func (rf *RetryFramework) getRetryObj(lockedKey string) (value *retryObjEntry, found bool) {
	return rf.retryEntries.Load(lockedKey)
}

func (rf *RetryFramework) DeleteRetryObj(lockedKey string) {
	rf.retryEntries.Delete(lockedKey)
}

// setRetryObjWithNoBackoff sets an object's backoff to be retried
// immediately during the next retry iteration
// Used only for testing right now
func (rf *RetryFramework) setRetryObjWithNoBackoff(entry *retryObjEntry) {
	entry.backoffSec = noBackoff
}

// removeDeleteFromRetryObj removes any old object from a retry entry
func (rf *RetryFramework) removeDeleteFromRetryObj(entry *retryObjEntry) {
	entry.oldObj = nil
	entry.config = nil
}

// increaseFailedAttemptsCounter increases by one the counter of failed add/update/delete attempts
// for the given key
func (rf *RetryFramework) increaseFailedAttemptsCounter(entry *retryObjEntry) {
	entry.failedAttempts++
}

// RequestRetryFramework allows a caller to immediately request to iterate through all objects that
// are in the retry cache. This will ignore any outstanding time wait/backoff state
func (rf *RetryFramework) RequestRetryObjs() {
	select {
	case rf.retryChan <- struct{}{}:
		klog.V(5).Infof("Iterate retry objects requested (resource %s)", rf.ObjType)
	default:
		klog.V(5).Infof("Iterate retry objects already requested (resource %s)", rf.ObjType)
	}
}

// var Sep = "/"

// // return cache.SplitMetaNamespaceKey
// func splitNamespacedName(namespacedName string) (string, string) {
// 	if strings.Contains(namespacedName, sep) {
// 		s := strings.SplitN(namespacedName, sep, 2)
// 		if len(s) == 2 {
// 			return s[0], s[1]
// 		}
// 	}
// 	return namespacedName, ""
// }

// // ?
// func getNamespacedName(namespace, name string) string {
// 	return namespace + sep + name
// }

// // hasResourceAnUpdateFunc returns true if the given resource type has a dedicated update function.
// // It returns false if, upon an update event on this resource type, we instead need to first delete the old
// // object and then add the new one.
// func hasResourceAnUpdateFunc(objType reflect.Type) bool {
// 	switch objType {
// 	case factory.PodType,
// 		factory.NodeType,
// 		factory.PeerPodSelectorType,
// 		factory.PeerPodForNamespaceAndPodSelectorType,
// 		factory.EgressIPType,
// 		factory.EgressIPNamespaceType,
// 		factory.EgressIPPodType,
// 		factory.EgressNodeType,
// 		factory.CloudPrivateIPConfigType,
// 		factory.LocalPodSelectorType:
// 		return true
// 	}
// 	return false
// }

// Given an object and its type, it returns the key for this object and an error if the key retrieval failed.
// For all namespaced resources, the key will be namespace/name. For resource types without a namespace,
// the key will be the object name itself.
func GetResourceKey(objType reflect.Type, obj interface{}) (string, error) {
	// TODO first validate that this works for ALL types (run at least the unit tests),
	// then replace this directly with cache.MetaNamespaceKeyFunc everywhere
	return cache.MetaNamespaceKeyFunc(obj)

	// switch objType {
	// case factory.PolicyType:
	// 	np, ok := obj.(*knet.NetworkPolicy)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *knet.NetworkPolicy", obj)
	// 	}
	// 	return getPolicyNamespacedName(np), nil

	// case factory.NodeType,
	// 	factory.EgressNodeType:
	// 	node, ok := obj.(*kapi.Node)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Node", obj)
	// 	}
	// 	return node.Name, nil

	// case factory.PeerServiceType:
	// 	service, ok := obj.(*kapi.Service)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Service", obj)
	// 	}
	// 	return getNamespacedName(service.Namespace, service.Name), nil

	// case factory.PodType,
	// 	factory.PeerPodSelectorType,
	// 	factory.PeerPodForNamespaceAndPodSelectorType,
	// 	factory.LocalPodSelectorType,
	// 	factory.EgressIPPodType:
	// 	pod, ok := obj.(*kapi.Pod)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Pod", obj)
	// 	}
	// 	return getNamespacedName(pod.Namespace, pod.Name), nil

	// case factory.PeerNamespaceAndPodSelectorType,
	// 	factory.PeerNamespaceSelectorType,
	// 	factory.EgressIPNamespaceType:
	// 	namespace, ok := obj.(*kapi.Namespace)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Namespace", obj)
	// 	}
	// 	return namespace.Name, nil

	// case factory.EgressFirewallType:
	// 	egressFirewall, ok := obj.(*egressfirewall.EgressFirewall)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *egressfirewall.EgressFirewall", obj)
	// 	}
	// 	return getEgressFirewallNamespacedName(egressFirewall), nil

	// case factory.EgressIPType:
	// 	eIP, ok := obj.(*egressipv1.EgressIP)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *egressipv1.EgressIP", obj)
	// 	}
	// 	return eIP.Name, nil
	// case factory.CloudPrivateIPConfigType:
	// 	cloudPrivateIPConfig, ok := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
	// 	if !ok {
	// 		return "", fmt.Errorf("could not cast %T object to *ocpcloudnetworkapi.CloudPrivateIPConfig", obj)
	// 	}
	// 	return cloudPrivateIPConfig.Name, nil
	// }

	// return "", fmt.Errorf("object type %s not supported", objType)
}

// // Given an object and its type, getInternalCacheEntry returns the internal cache entry for this object.
// // This is now used only for pods, which will get their the logical port cache entry.
// // TODO this should be filled in by obj_retry_master.go when initializing retry for ovnk master
// // HOW??? Should each resource type create a struct, fill it with some functions, and then in obj_retry.go
// // I define generic functions over that struct? Yes

// // TODO remove this and define the code snippet below in obj_retry_master when initializing a struct for pods
// func (oc *Controller) GetInternalCacheEntry(objType reflect.Type, obj interface{}) interface{} {
// 	switch objType {
// 	case factory.PodType:
// 		pod := obj.(*kapi.Pod)
// 		return oc.getPortInfo(pod)
// 	default:
// 		return nil
// 	}
// }

// // Given an object key and its type, getResourceFromInformerCache returns the latest state of the object
// // from the informers cache.
// // TODO In the generic pkg, it only needs access to the watchFactory
// // TODO Maybe have a global overall retry struct initialized with a watchFactory instance? Then here you'd call only that
// // it would have whatever is necessary for both master and ovn, that is an intersection of their controller attributes.
// func (rf *RetryFramework) getResourceFromInformerCache(key string) (interface{}, error) {
// 	var obj interface{}
// 	var err error

// 	switch rf.ObjType {
// 	case factory.PolicyType:
// 		namespace, name := splitNamespacedName(key)
// 		obj, err = rf.watchFactory.GetNetworkPolicy(namespace, name)

// 	case factory.NodeType,
// 		factory.EgressNodeType:
// 		obj, err = rf.watchFactory.GetNode(key)

// 	case factory.PeerServiceType:
// 		namespace, name := splitNamespacedName(key)
// 		obj, err = rf.watchFactory.GetService(namespace, name)

// 	case factory.PodType,
// 		factory.PeerPodSelectorType,
// 		factory.PeerPodForNamespaceAndPodSelectorType,
// 		factory.LocalPodSelectorType,
// 		factory.EgressIPPodType:
// 		namespace, name := splitNamespacedName(key)
// 		obj, err = rf.watchFactory.GetPod(namespace, name)

// 	case factory.PeerNamespaceAndPodSelectorType,
// 		factory.PeerNamespaceSelectorType,
// 		factory.EgressIPNamespaceType:
// 		obj, err = rf.watchFactory.GetNamespace(key)

// 	case factory.EgressFirewallType:
// 		namespace, name := splitNamespacedName(key)
// 		obj, err = rf.watchFactory.GetEgressFirewall(namespace, name)

// 	case factory.EgressIPType:
// 		obj, err = rf.watchFactory.GetEgressIP(key)

// 	case factory.CloudPrivateIPConfigType:
// 		obj, err = rf.watchFactory.GetCloudPrivateIPConfig(key)

// 	default:
// 		err = fmt.Errorf("object type %s not supported, cannot retrieve it from informers cache",
// 			objType)
// 	}
// 	return obj, err
// }

// // Given an object and its type, recordAddEvent records the add event on this object.
// // TODO in obj_retry_master, you need to set the recordAddEventFunction for each concerned resource
// func (oc *Controller) recordAddEvent(objType reflect.Type, obj interface{}) {
// 	switch objType {
// 	case factory.PodType:
// 		klog.V(5).Infof("Recording add event on pod")
// 		pod := obj.(*kapi.Pod)
// 		oc.PodRecorder.AddPod(pod.UID)
// 		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
// 	case factory.PolicyType:
// 		klog.V(5).Infof("Recording add event on network policy")
// 		np := obj.(*knet.NetworkPolicy)
// 		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
// 	}
// }

// // Given an object and its type, recordUpdateEvent records the update event on this object.
// func recordUpdateEvent(objType reflect.Type, obj interface{}) {
// 	switch objType {
// 	case factory.PodType:
// 		klog.V(5).Infof("Recording update event on pod")
// 		pod := obj.(*kapi.Pod)
// 		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
// 	case factory.PolicyType:
// 		klog.V(5).Infof("Recording update event on network policy")
// 		np := obj.(*knet.NetworkPolicy)
// 		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
// 	}
// }

// // Given an object and its type, recordDeleteEvent records the delete event on this object. Only used for pods now.
// // TODO in obj_retry_master.go, you need to pass the pod-recorder initialized in master or pass the CleanPod function for pods
// func (rf *RetryFramework) recordDeleteEvent(objType reflect.Type, obj interface{}) {
// 	switch objType {
// 	case factory.PodType:
// 		klog.V(5).Infof("Recording delete event on pod")
// 		pod := obj.(*kapi.Pod)
// 		rf.PodRecorder.CleanPod(pod.UID)
// 		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
// 	case factory.PolicyType:
// 		klog.V(5).Infof("Recording delete event on network policy")
// 		np := obj.(*knet.NetworkPolicy)
// 		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
// 	}
// }

// func recordSuccessEvent(objType reflect.Type, obj interface{}) {
// 	switch objType {
// 	case factory.PodType:
// 		klog.V(5).Infof("Recording success event on pod")
// 		pod := obj.(*kapi.Pod)
// 		metrics.GetConfigDurationRecorder().End("pod", pod.Namespace, pod.Name)
// 	case factory.PolicyType:
// 		klog.V(5).Infof("Recording success event on network policy")
// 		np := obj.(*knet.NetworkPolicy)
// 		metrics.GetConfigDurationRecorder().End("networkpolicy", np.Namespace, np.Name)
// 	}
// }

// // Given an object and its type, recordErrorEvent records an error event on this object.
// // Only used for pods now.
// // // TODO in obj_retry_master.go, you need to pass the snippet below
// func (oc *Controller) recordErrorEvent(objType reflect.Type, obj interface{}, err error) {
// 	switch objType {
// 	case factory.PodType:
// 		klog.V(5).Infof("Recording error event on pod")
// 		pod := obj.(*kapi.Pod)
// 		oc.recordPodEvent(err, pod)
// 	}
// }

// Given an object and its type, isResourceScheduled returns true if the object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
// TODO ok as is
// func isResourceScheduled(objType reflect.Type, obj interface{}) bool { //
// 	switch objType {	//
// 	case factory.PodType:
// 		pod := obj.(*kapi.Pod)
// 		return util.PodScheduled(pod)
// 	}
// 	return true
// }

// Given an object type, resourceNeedsUpdate returns true if the object needs to invoke update during iterate retry.
// func resourceNeedsUpdate(objType reflect.Type) bool {
// 	switch objType {
// 	case factory.EgressNodeType,
// 		factory.EgressIPType,
// 		factory.EgressIPPodType,
// 		factory.EgressIPNamespaceType,
// 		factory.CloudPrivateIPConfigType:
// 		return true
// 	}
// 	return false
// }

// Given a *RetryFramework instance, an object to add and a boolean specifying if the function was executed from
// iterateRetryResources, addResource adds the specified object to the cluster according to its type and
// returns the error, if any, yielded during object creation.
// func (oc *Controller) addResource(rf *RetryFramework, obj interface{}, fromRetryLoop bool) error {
// 	var err error

// 	switch rf.ObjType {
// 	case factory.PodType:
// 		pod, ok := obj.(*kapi.Pod)
// 		if !ok {
// 			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
// 		}
// 		return oc.ensurePod(nil, pod, true)

// 	case factory.PolicyType:
// 		np, ok := obj.(*knet.NetworkPolicy)
// 		if !ok {
// 			return fmt.Errorf("could not cast %T object to *knet.NetworkPolicy", obj)
// 		}

// 		if err = oc.addNetworkPolicy(np); err != nil {
// 			klog.Infof("Network Policy add failed for %s/%s, will try again later: %v",
// 				np.Namespace, np.Name, err)
// 			return err
// 		}

// 	case factory.NodeType:
// 		node, ok := obj.(*kapi.Node)
// 		if !ok {
// 			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
// 		}
// 		var nodeParams *nodeSyncs
// 		if fromRetryLoop {
// 			_, nodeSync := oc.addNodeFailed.Load(node.Name)
// 			_, clusterRtrSync := oc.nodeClusterRouterPortFailed.Load(node.Name)
// 			_, mgmtSync := oc.mgmtPortFailed.Load(node.Name)
// 			_, gwSync := oc.gatewaysFailed.Load(node.Name)
// 			nodeParams = &nodeSyncs{
// 				nodeSync,
// 				clusterRtrSync,
// 				mgmtSync,
// 				gwSync}
// 		} else {
// 			nodeParams = &nodeSyncs{true, true, true, true}
// 		}

// 		if err = oc.addUpdateNodeEvent(node, nodeParams); err != nil {
// 			klog.Infof("Node add failed for %s, will try again later: %v",
// 				node.Name, err)
// 			return err
// 		}

// 	case factory.PeerServiceType:
// 		service, ok := obj.(*kapi.Service)
// 		if !ok {
// 			return fmt.Errorf("could not cast peer service of type %T to *kapi.Service", obj)
// 		}
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerServiceAdd(extraParameters.gp, service)

// 	case factory.PeerPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, obj)

// 	case factory.PeerNamespaceAndPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		namespace := obj.(*kapi.Namespace)
// 		extraParameters.np.RLock()
// 		alreadyDeleted := extraParameters.np.deleted
// 		extraParameters.np.RUnlock()
// 		if alreadyDeleted {
// 			return nil
// 		}

// 		// start watching pods in this namespace and selected by the label selector in extraParameters.podSelector
// 		retryPeerPods := NewRetryFramework(
// 			factory.PeerPodForNamespaceAndPodSelectorType,
// 			namespace.Name,
// 			extraParameters.podSelector,
// 			nil,
// 			&NetworkPolicyExtraParameters{gp: extraParameters.gp},
// 		)
// 		// The AddFilteredPodHandler call might call handlePeerPodSelectorAddUpdate
// 		// on existing pods so we can't be holding the lock at this point
// 		podHandler, err := rf.WatchResource(retryPeerPods)
// 		if err != nil {
// 			klog.Errorf("Failed WatchResource for PeerNamespaceAndPodSelectorType: %v", err)
// 			return err
// 		}

// 		extraParameters.np.Lock()
// 		defer extraParameters.np.Unlock()
// 		if extraParameters.np.deleted {
// 			oc.watchFactory.RemovePodHandler(podHandler)
// 			return nil
// 		}
// 		extraParameters.np.podHandlerList = append(extraParameters.np.podHandlerList, podHandler)

// 	case factory.PeerPodForNamespaceAndPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, obj)

// 	case factory.PeerNamespaceSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		namespace := obj.(*kapi.Namespace)
// 		// Update the ACL ...
// 		return oc.handlePeerNamespaceSelectorOnUpdate(extraParameters.np, extraParameters.gp, func() bool {
// 			// ... on condition that the added address set was not already in the 'gress policy
// 			return extraParameters.gp.addNamespaceAddressSet(namespace.Name)
// 		})

// 	case factory.LocalPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handleLocalPodSelectorAddFunc(
// 			extraParameters.policy,
// 			extraParameters.np,
// 			extraParameters.portGroupIngressDenyName,
// 			extraParameters.portGroupEgressDenyName,
// 			obj)

// 	case factory.EgressFirewallType:
// 		var err error
// 		egressFirewall := obj.(*egressfirewall.EgressFirewall).DeepCopy()
// 		if err = oc.addEgressFirewall(egressFirewall); err != nil {
// 			egressFirewall.Status.Status = egressFirewallAddError
// 		} else {
// 			egressFirewall.Status.Status = egressFirewallAppliedCorrectly
// 			metrics.UpdateEgressFirewallRuleCount(float64(len(egressFirewall.Spec.Egress)))
// 			metrics.IncrementEgressFirewallCount()
// 		}
// 		if err := oc.updateEgressFirewallStatusWithRetry(egressFirewall); err != nil {
// 			klog.Errorf("Failed to update egress firewall status %s, error: %v", getEgressFirewallNamespacedName(egressFirewall), err)
// 		}
// 		return err

// 	case factory.EgressIPType:
// 		eIP := obj.(*egressipv1.EgressIP)
// 		return oc.reconcileEgressIP(nil, eIP)

// 	case factory.EgressIPNamespaceType:
// 		namespace := obj.(*kapi.Namespace)
// 		return oc.reconcileEgressIPNamespace(nil, namespace)

// 	case factory.EgressIPPodType:
// 		pod := obj.(*kapi.Pod)
// 		return oc.reconcileEgressIPPod(nil, pod)

// 	case factory.EgressNodeType:
// 		node := obj.(*kapi.Node)
// 		if err := oc.setupNodeForEgress(node); err != nil {
// 			return err
// 		}
// 		nodeEgressLabel := util.GetNodeEgressLabel()
// 		nodeLabels := node.GetLabels()
// 		_, hasEgressLabel := nodeLabels[nodeEgressLabel]
// 		if hasEgressLabel {
// 			oc.setNodeEgressAssignable(node.Name, true)
// 		}
// 		isReady := oc.isEgressNodeReady(node)
// 		if isReady {
// 			oc.setNodeEgressReady(node.Name, true)
// 		}
// 		isReachable := oc.isEgressNodeReachable(node)
// 		if isReachable {
// 			oc.setNodeEgressReachable(node.Name, true)
// 		}
// 		if hasEgressLabel && isReachable && isReady {
// 			if err := oc.addEgressNode(node.Name); err != nil {
// 				return err
// 			}
// 		}

// 	case factory.CloudPrivateIPConfigType:
// 		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
// 		return oc.reconcileCloudPrivateIPConfig(nil, cloudPrivateIPConfig)

// 	default:
// 		return fmt.Errorf("no add function for object type %s", rf.ObjType)
// 	}

// 	return nil
// }

// // Given a *RetryFramework instance, an old and a new object, UpdateResource updates the specified object in the cluster
// // to its version in newObj according to its type and returns the error, if any, yielded during the object update.
// // The inRetryCache boolean argument is to indicate if the given resource is in the retryCache or not.
// func (oc *Controller) UpdateResource(rf *RetryFramework, oldObj, newObj interface{}, inRetryCache bool) error {
// 	switch rf.ObjType {
// 	case factory.PodType:
// 		oldPod := oldObj.(*kapi.Pod)
// 		newPod := newObj.(*kapi.Pod)

// 		return oc.ensurePod(oldPod, newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))

// 	case factory.NodeType:
// 		newNode, ok := newObj.(*kapi.Node)
// 		if !ok {
// 			return fmt.Errorf("could not cast newObj of type %T to *kapi.Node", newObj)
// 		}
// 		oldNode, ok := oldObj.(*kapi.Node)
// 		if !ok {
// 			return fmt.Errorf("could not cast oldObj of type %T to *kapi.Node", oldObj)
// 		}
// 		// determine what actually changed in this update
// 		_, nodeSync := oc.addNodeFailed.Load(newNode.Name)
// 		_, failed := oc.nodeClusterRouterPortFailed.Load(newNode.Name)
// 		clusterRtrSync := failed || nodeChassisChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)
// 		_, failed = oc.mgmtPortFailed.Load(newNode.Name)
// 		mgmtSync := failed || macAddressChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)
// 		_, failed = oc.gatewaysFailed.Load(newNode.Name)
// 		gwSync := (failed || gatewayChanged(oldNode, newNode) ||
// 			nodeSubnetChanged(oldNode, newNode) || hostAddressesChanged(oldNode, newNode))

// 		return oc.addUpdateNodeEvent(newNode, &nodeSyncs{nodeSync, clusterRtrSync, mgmtSync, gwSync})

// 	case factory.PeerPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, newObj)

// 	case factory.PeerPodForNamespaceAndPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, newObj)

// 	case factory.LocalPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handleLocalPodSelectorAddFunc(
// 			extraParameters.policy,
// 			extraParameters.np,
// 			extraParameters.portGroupIngressDenyName,
// 			extraParameters.portGroupEgressDenyName,
// 			newObj)

// 	case factory.EgressIPType:
// 		oldEIP := oldObj.(*egressipv1.EgressIP)
// 		newEIP := newObj.(*egressipv1.EgressIP)
// 		return oc.reconcileEgressIP(oldEIP, newEIP)

// 	case factory.EgressIPNamespaceType:
// 		oldNamespace := oldObj.(*kapi.Namespace)
// 		newNamespace := newObj.(*kapi.Namespace)
// 		return oc.reconcileEgressIPNamespace(oldNamespace, newNamespace)

// 	case factory.EgressIPPodType:
// 		oldPod := oldObj.(*kapi.Pod)
// 		newPod := newObj.(*kapi.Pod)
// 		return oc.reconcileEgressIPPod(oldPod, newPod)

// 	case factory.EgressNodeType:
// 		oldNode := oldObj.(*kapi.Node)
// 		newNode := newObj.(*kapi.Node)
// 		// Initialize the allocator on every update,
// 		// ovnkube-node/cloud-network-config-controller will make sure to
// 		// annotate the node with the egressIPConfig, but that might have
// 		// happened after we processed the ADD for that object, hence keep
// 		// retrying for all UPDATEs.
// 		if err := oc.initEgressIPAllocator(newNode); err != nil {
// 			klog.Warningf("Egress node initialization error: %v", err)
// 		}
// 		nodeEgressLabel := util.GetNodeEgressLabel()
// 		oldLabels := oldNode.GetLabels()
// 		newLabels := newNode.GetLabels()
// 		_, oldHadEgressLabel := oldLabels[nodeEgressLabel]
// 		_, newHasEgressLabel := newLabels[nodeEgressLabel]
// 		// If the node is not labeled for egress assignment, just return
// 		// directly, we don't really need to set the ready / reachable
// 		// status on this node if the user doesn't care about using it.
// 		if !oldHadEgressLabel && !newHasEgressLabel {
// 			return nil
// 		}
// 		if oldHadEgressLabel && !newHasEgressLabel {
// 			klog.Infof("Node: %s has been un-labeled, deleting it from egress assignment", newNode.Name)
// 			oc.setNodeEgressAssignable(oldNode.Name, false)
// 			return oc.deleteEgressNode(oldNode.Name)
// 		}
// 		isOldReady := oc.isEgressNodeReady(oldNode)
// 		isNewReady := oc.isEgressNodeReady(newNode)
// 		isNewReachable := oc.isEgressNodeReachable(newNode)
// 		oc.setNodeEgressReady(newNode.Name, isNewReady)
// 		oc.setNodeEgressReachable(newNode.Name, isNewReachable)
// 		if !oldHadEgressLabel && newHasEgressLabel {
// 			klog.Infof("Node: %s has been labeled, adding it for egress assignment", newNode.Name)
// 			oc.setNodeEgressAssignable(newNode.Name, true)
// 			if isNewReady && isNewReachable {
// 				if err := oc.addEgressNode(newNode.Name); err != nil {
// 					return err
// 				}
// 			} else {
// 				klog.Warningf("Node: %s has been labeled, but node is not ready and reachable, cannot use it for egress assignment", newNode.Name)
// 			}
// 			return nil
// 		}
// 		if isOldReady == isNewReady {
// 			return nil
// 		}
// 		if !isNewReady {
// 			klog.Warningf("Node: %s is not ready, deleting it from egress assignment", newNode.Name)
// 			if err := oc.deleteEgressNode(newNode.Name); err != nil {
// 				return err
// 			}
// 		} else if isNewReady && isNewReachable {
// 			klog.Infof("Node: %s is ready and reachable, adding it for egress assignment", newNode.Name)
// 			if err := oc.addEgressNode(newNode.Name); err != nil {
// 				return err
// 			}
// 		}
// 		return nil

// 	case factory.CloudPrivateIPConfigType:
// 		oldCloudPrivateIPConfig := oldObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
// 		newCloudPrivateIPConfig := newObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
// 		return oc.reconcileCloudPrivateIPConfig(oldCloudPrivateIPConfig, newCloudPrivateIPConfig)
// 	}

// 	return fmt.Errorf("no update function for object type %s", rf.ObjType)
// }

// // Given a *RetryFramework instance, an object and optionally a cachedObj, deleteResource deletes the object from the cluster
// // according to the delete logic of its resource type. cachedObj is the internal cache entry for this object,
// // used for now for pods and network policies.
// func (oc *Controller) deleteResource(rf *RetryFramework, obj, cachedObj interface{}) error {
// 	switch rf.ObjType {
// 	case factory.PodType:
// 		var portInfo *lpInfo
// 		pod := obj.(*kapi.Pod)

// 		if cachedObj != nil {
// 			portInfo = cachedObj.(*lpInfo)
// 		}
// 		oc.logicalPortCache.remove(util.GetLogicalPortName(pod.Namespace, pod.Name))
// 		return oc.removePod(pod, portInfo)

// 	case factory.PolicyType:
// 		var cachedNP *networkPolicy
// 		knp, ok := obj.(*knet.NetworkPolicy)
// 		if !ok {
// 			return fmt.Errorf("could not cast obj of type %T to *knet.NetworkPolicy", obj)
// 		}

// 		if cachedObj != nil {
// 			if cachedNP, ok = cachedObj.(*networkPolicy); !ok {
// 				cachedNP = nil
// 			}
// 		}
// 		return oc.deleteNetworkPolicy(knp, cachedNP)

// 	case factory.NodeType:
// 		node, ok := obj.(*kapi.Node)
// 		if !ok {
// 			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
// 		}
// 		return oc.deleteNodeEvent(node)

// 	case factory.PeerServiceType:
// 		service, ok := obj.(*kapi.Service)
// 		if !ok {
// 			return fmt.Errorf("could not cast peer service of type %T to *kapi.Service", obj)
// 		}
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerServiceDelete(extraParameters.gp, service)

// 	case factory.PeerPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerPodSelectorDelete(extraParameters.gp, obj)

// 	case factory.PeerNamespaceAndPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		// when the namespace labels no longer apply
// 		// remove the namespaces pods from the address_set
// 		var errs []error
// 		namespace := obj.(*kapi.Namespace)
// 		pods, _ := oc.watchFactory.GetPods(namespace.Name)

// 		for _, pod := range pods {
// 			if err := oc.handlePeerPodSelectorDelete(extraParameters.gp, pod); err != nil {
// 				errs = append(errs, err)
// 			}
// 		}
// 		return kerrorsutil.NewAggregate(errs)

// 	case factory.PeerPodForNamespaceAndPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handlePeerPodSelectorDelete(extraParameters.gp, obj)

// 	case factory.PeerNamespaceSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		namespace := obj.(*kapi.Namespace)
// 		// Remove namespace address set from the *gress policy in cache
// 		// (done in gress.delNamespaceAddressSet()), and then update ACLs
// 		return oc.handlePeerNamespaceSelectorOnUpdate(extraParameters.np, extraParameters.gp, func() bool {
// 			// ... on condition that the removed address set was in the 'gress policy
// 			return extraParameters.gp.delNamespaceAddressSet(namespace.Name)
// 		})

// 	case factory.PeerPodForNamespaceAndPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handleLocalPodSelectorDelFunc(
// 			extraParameters.policy,
// 			extraParameters.np,
// 			extraParameters.portGroupIngressDenyName,
// 			extraParameters.portGroupEgressDenyName,
// 			obj)

// 	case factory.LocalPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		return oc.handleLocalPodSelectorDelFunc(
// 			extraParameters.policy,
// 			extraParameters.np,
// 			extraParameters.portGroupIngressDenyName,
// 			extraParameters.portGroupEgressDenyName,
// 			obj)

// 	case factory.EgressFirewallType:
// 		egressFirewall := obj.(*egressfirewall.EgressFirewall)
// 		if err := oc.deleteEgressFirewall(egressFirewall); err != nil {
// 			return err
// 		}
// 		metrics.UpdateEgressFirewallRuleCount(float64(-len(egressFirewall.Spec.Egress)))
// 		metrics.DecrementEgressFirewallCount()
// 		return nil

// 	case factory.EgressIPType:
// 		eIP := obj.(*egressipv1.EgressIP)
// 		return oc.reconcileEgressIP(eIP, nil)

// 	case factory.EgressIPNamespaceType:
// 		namespace := obj.(*kapi.Namespace)
// 		return oc.reconcileEgressIPNamespace(namespace, nil)

// 	case factory.EgressIPPodType:
// 		pod := obj.(*kapi.Pod)
// 		return oc.reconcileEgressIPPod(pod, nil)

// 	case factory.EgressNodeType:
// 		node := obj.(*kapi.Node)
// 		if err := oc.deleteNodeForEgress(node); err != nil {
// 			return err
// 		}
// 		nodeEgressLabel := util.GetNodeEgressLabel()
// 		nodeLabels := node.GetLabels()
// 		if _, hasEgressLabel := nodeLabels[nodeEgressLabel]; hasEgressLabel {
// 			if err := oc.deleteEgressNode(node.Name); err != nil {
// 				return err
// 			}
// 		}
// 		return nil

// 	case factory.CloudPrivateIPConfigType:
// 		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
// 		return oc.reconcileCloudPrivateIPConfig(cloudPrivateIPConfig, nil)

// 	default:
// 		return fmt.Errorf("object type %s not supported", rf.ObjType)
// 	}
// }

func (rf *RetryFramework) resourceRetry(objKey string, now time.Time) {
	rf.DoWithLock(objKey, func(key string) {
		entry, loaded := rf.getRetryObj(key)
		if !loaded {
			klog.V(5).Infof("%v resource %s was not found in the iterateRetryResources map while retrying resource setup", rf.ObjType, objKey)
			return
		}

		if entry.failedAttempts >= MaxFailedAttempts {
			klog.Warningf("Dropping retry entry for %s %s: exceeded number of failed attempts",
				rf.ObjType, objKey)
			rf.DeleteRetryObj(key)
			return
		}
		forceRetry := false
		// check if immediate retry is requested
		if entry.backoffSec == noBackoff {
			entry.backoffSec = initialBackoff
			forceRetry = true
		}
		backoff := (entry.backoffSec * time.Second) + (time.Duration(rand.Intn(500)) * time.Millisecond)
		objTimer := entry.timeStamp.Add(backoff)
		if !forceRetry && now.Before(objTimer) {
			klog.V(5).Infof("Attempting retry of %s %s before timer (time: %s): skip", rf.ObjType, objKey, objTimer)
			return
		}

		// update backoff for future attempts in case of failure
		entry.backoffSec = entry.backoffSec * 2
		if entry.backoffSec > 60 {
			entry.backoffSec = 60
		}

		// storing original obj for metrics
		var initObj interface{}
		if entry.newObj != nil {
			initObj = entry.newObj
		} else if entry.oldObj != nil {
			initObj = entry.oldObj
		}

		klog.Infof("Retry object setup: %s %s", rf.ObjType, objKey)

		if entry.newObj != nil {
			// get the latest version of the object from the informer;
			// if it doesn't exist we are not going to create the new object.
			kObj, err := rf.GetResourceFromInformerCache(objKey) // TODO this won't need oc
			if err != nil {
				if kerrors.IsNotFound(err) {
					klog.Infof("%s %s not found in the informers cache,"+
						" not going to retry object create", rf.ObjType, objKey)
					kObj = nil
				} else {
					klog.Errorf("Failed to look up %s %s in the informers cache,"+
						" will retry later: %v", rf.ObjType, objKey, err)
					return
				}
			}
			entry.newObj = kObj
		}
		if rf.ResourceNeedsUpdate() && entry.config != nil && entry.newObj != nil {
			klog.Infof("%v retry: updating object %s", rf.ObjType, objKey)
			if err := rf.UpdateResource(rf, entry.config, entry.newObj, true); err != nil {
				klog.Infof("%v retry update failed for %s, will try again later: %v", rf.ObjType, objKey, err)
				entry.timeStamp = time.Now()
				entry.failedAttempts++
				return
			}
			// successfully cleaned up new and old object, remove it from the retry cache
			entry.newObj = nil
			entry.config = nil
		} else {
			// delete old object if needed
			if entry.oldObj != nil {
				klog.Infof("Removing old object: %s %s (failed: %s)",
					rf.ObjType, objKey, entry.failedAttempts)
				if !rf.IsResourceScheduled(entry.oldObj) {
					klog.V(5).Infof("Retry: %s %s not scheduled", rf.ObjType, objKey)
					entry.failedAttempts++
					klog.Infof("Failed to remove old object: %s %s (failed: %s)",
						rf.ObjType, objKey, entry.failedAttempts)

					return
				}
				if err := rf.DeleteResource(rf, entry.oldObj, entry.config); err != nil {
					klog.Infof("Retry delete failed for %s %s, will try again later: %v",
						rf.ObjType, objKey, err)

					entry.timeStamp = time.Now()
					entry.failedAttempts++
					klog.Infof("Failed2 to remove old object: %s %s (failed: %s)",
						rf.ObjType, objKey, entry.failedAttempts)
					time.Sleep(2 * time.Second)
					return
				}
				// successfully cleaned up old object, remove it from the retry cache
				entry.oldObj = nil
			}

			// create new object if needed
			if entry.newObj != nil {
				klog.Infof("Adding new object: %s %s", rf.ObjType, objKey)
				if !rf.IsResourceScheduled(entry.newObj) {
					klog.V(5).Infof("Retry: %s %s not scheduled", rf.ObjType, objKey)
					entry.failedAttempts++
					return
				}
				if err := rf.AddResource(rf, entry.newObj, true); err != nil {
					klog.Infof("Retry add failed for %s %s, will try again later: %v", rf.ObjType, objKey, err)
					entry.timeStamp = time.Now()
					entry.failedAttempts++
					return
				}
				// successfully cleaned up new object, remove it from the retry cache
				entry.newObj = nil
			}
		}

		klog.Infof("Retry successful for %s %s after %d failed attempt(s)", rf.ObjType, objKey, entry.failedAttempts)
		if initObj != nil {
			rf.RecordSuccessEvent(initObj)
		}
		rf.DeleteRetryObj(key)
	})
}

// iterateRetryResources checks if any outstanding resource objects exist and if so it tries to
// re-add them. updateAll forces all objects to be attempted to be retried regardless.
// iterateRetryResources makes a snapshot of keys present in the rf.retryEntries cache, and runs retry only
// for those keys. New changes may be applied to saved keys entries while iterateRetryResources is executed.
// Deleted entries will be ignored, and all the updates will be reflected with key Lock.
// Keys added after the snapshot was done won't be retried during this run.
func (rf *RetryFramework) iterateRetryResources() {
	now := time.Now()
	wg := &sync.WaitGroup{}

	entriesKeys := rf.retryEntries.GetKeys()
	// Now process the above list of pods that need re-try by holding the lock for each one of them.
	klog.Infof("Going to retry %v resource setup for %d number of resources: %s", rf.ObjType, len(entriesKeys), entriesKeys) // TODO was v(5)

	for _, entryKey := range entriesKeys {
		wg.Add(1)
		go func(entryKey string) {
			defer wg.Done()
			rf.resourceRetry(entryKey, now)
		}(entryKey)
	}
	klog.V(5).Infof("Waiting for all the %s retry setup to complete in iterateRetryResources", rf.ObjType)
	wg.Wait()
	klog.V(5).Infof("Function iterateRetryResources ended (in %v)", time.Since(now))
}

// periodicallyRetryResources tracks RetryFramework and checks if any object needs to be retried for add or delete every
// RetryObjInterval seconds or when requested through retryChan.
func (rf *RetryFramework) periodicallyRetryResources() {
	klog.Infof("Starting periodicallyRetryResources")
	timer := time.NewTicker(RetryObjInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			klog.Infof("periodicallyRetryResources: timer is triggered")
			rf.iterateRetryResources()

		case <-rf.retryChan:
			klog.Infof("periodicallyRetryResources: Retry channel got triggered: retrying failed objects of type %s", rf.ObjType) // was V(5)
			rf.iterateRetryResources()
			timer.Reset(RetryObjInterval)

		case <-rf.StopChan: // TODO initialize this in retry_framework struct
			klog.V(5).Infof("Stop channel got triggered: will stop retrying failed objects of type %s", rf.ObjType)
			return
		}
	}
}

// // Given a *RetryFramework instance, getSyncResourcesFunc retuns the sync function for a given resource type.
// // This will be then called on all existing objects when a watcher is started.
// // TODO how can I generalize this? the problem is the access to oc...
// // Here oc should NOT appear, while in obj_retry_master it should... boh.
// func (oc *Controller) getSyncResourcesFunc(rf *RetryFramework) (func([]interface{}) error, error) {

// 	var syncFunc func([]interface{}) error

// 	switch rf.ObjType {
// 	case factory.PodType:
// 		syncFunc = oc.syncPodsRetriable

// 	case factory.PolicyType:
// 		syncFunc = oc.syncNetworkPolicies

// 	case factory.NodeType:
// 		syncFunc = oc.syncNodesRetriable

// 	case factory.PeerServiceType,
// 		factory.PeerNamespaceAndPodSelectorType:
// 		syncFunc = nil

// 	case factory.PeerPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		syncFunc = func(objs []interface{}) error {
// 			return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, objs...)
// 		}

// 	case factory.PeerPodForNamespaceAndPodSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		syncFunc = func(objs []interface{}) error {
// 			return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, objs...)
// 		}

// 	case factory.PeerNamespaceSelectorType:
// 		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
// 		// the function below will never fail, so there's no point in making it retriable...
// 		syncFunc = func(i []interface{}) error {
// 			// This needs to be a write lock because there's no locking around 'gress policies
// 			extraParameters.np.Lock()
// 			defer extraParameters.np.Unlock()
// 			// We load the existing address set into the 'gress policy.
// 			// Notice that this will make the AddFunc for this initial
// 			// address set a noop.
// 			// The ACL must be set explicitly after setting up this handler
// 			// for the address set to be considered.
// 			extraParameters.gp.addNamespaceAddressSets(i)
// 			return nil
// 		}

// 	case factory.LocalPodSelectorType:
// 		syncFunc = rf.syncFunc

// 	case factory.EgressFirewallType:
// 		syncFunc = oc.syncEgressFirewall

// 	case factory.EgressIPType:
// 		syncFunc = oc.syncEgressIPs

// 	case factory.EgressNodeType:
// 		syncFunc = oc.initClusterEgressPolicies

// 	case factory.EgressIPPodType,
// 		factory.EgressIPNamespaceType,
// 		factory.CloudPrivateIPConfigType:
// 		syncFunc = nil

// 	default:
// 		return nil, fmt.Errorf("no sync function for object type %s", rf.ObjType)
// 	}

// 	return syncFunc, nil
// }

// // Given an object and its type, isObjectInTerminalState returns true if the object is a in terminal state.
// // This is used now for pods that are either in a PodSucceeded or in a PodFailed state.
// func isObjectInTerminalState(objType reflect.Type, obj interface{}) bool {
// 	switch objType {
// 	case factory.PodType,
// 		factory.PeerPodSelectorType,
// 		factory.PeerPodForNamespaceAndPodSelectorType,
// 		factory.LocalPodSelectorType,
// 		factory.EgressIPPodType:
// 		pod := obj.(*kapi.Pod)
// 		return util.PodCompleted(pod)

// 	default:
// 		return false
// 	}
// }

type resourceEvent string

var (
	resourceEventAdd    resourceEvent = "add"
	resourceEventUpdate resourceEvent = "update"
)

// processObjectInTerminalState is executed when an object has been added or updated and is actually in a terminal state
// already. The add or update event is not valid for such object, which we now remove from the cluster in order to
// free its resources. (for now, this applies to completed pods)
// processObjectInTerminalState doesn't unlock key
func (rf *RetryFramework) processObjectInTerminalState(obj interface{}, lockedKey string, event resourceEvent) {
	// The object is in a terminal state: delete it from the cluster, delete its retry entry and return.
	klog.Infof("Detected object %s of type %s in terminal state (e.g. completed)"+
		" during %s event: will remove it", lockedKey, rf.ObjType, event)

	internalCacheEntry := rf.GetInternalCacheEntry(obj)
	retryEntry := rf.InitRetryObjWithDelete(obj, lockedKey, internalCacheEntry, true) // set up the retry obj for deletion
	if err := rf.DeleteResource(rf, obj, internalCacheEntry); err != nil {
		klog.Errorf("Failed to delete object %s of type %s in terminal state, during %s event: %v",
			lockedKey, rf.ObjType, event, err)
		rf.RecordErrorEvent(obj, err)
		rf.increaseFailedAttemptsCounter(retryEntry)
		return
	}
	rf.DeleteRetryObj(lockedKey)
}

// WatchResource starts the watching of a resource type, manages its retry entries and calls
// back the appropriate handler logic. It also starts a goroutine that goes over all retry objects
// periodically or when explicitly requested.
// Note: when applying WatchResource to a new resource type, the appropriate resource-specific logic must be added to the
// the different methods it calls.
func (rf *RetryFramework) WatchResource() (*factory.Handler, error) {
	addHandlerFunc, err := rf.watchFactory.GetResourceHandlerFunc(rf.ObjType) // ok, call watchFactory in your retry_framework struct
	if err != nil {
		return nil, fmt.Errorf("no resource handler function found for resource %v. "+
			"Cannot watch this resource", rf.ObjType)
	}
	// syncFunc, err := oc.getSyncResourcesFunc(rf)
	// if err != nil {
	// 	return nil, fmt.Errorf("no sync function found for resource %v. "+
	// 		"Cannot watch this resource", rf.ObjType)
	// }

	// create the actual watcher
	handler, err := addHandlerFunc(
		rf.namespaceForFilteredHandler,     // filter out objects not in this namespace
		rf.labelSelectorForFilteredHandler, // filter out objects not matching these labels
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				rf.RecordAddEvent(obj)

				key, err := GetResourceKey(rf.ObjType, obj)
				if err != nil {
					klog.Errorf("Upon add event: %v", err)
					return
				}
				klog.V(5).Infof("Add event received for %s, key=%s", rf.ObjType, key)

				rf.DoWithLock(key, func(key string) {
					// This only applies to pod watchers (pods + dynamic network policy handlers watching pods):
					// if ovnkube-master is restarted, it will get all the add events with completed pods
					if rf.IsObjectInTerminalState(obj) {
						rf.processObjectInTerminalState(obj, key, resourceEventAdd)
						return
					}

					retryObj := rf.initRetryObjWithAdd(obj, key)
					// If there is a delete entry with the same key, we got an add event for an object
					// with the same name as a previous object that failed deletion.
					// Destroy the old object before we add the new one.
					//
					// Note it is okay to access retryEntry without lock here, as the entry is either
					// accessed by iterateRetryResources or in add/update/delete handler of this resource;
					// the former is prevented as its ignore field is set to true, and the latter is serialized.
					if retryObj.oldObj != nil {
						klog.Infof("Detected stale object during new object"+
							" add of type %s with the same key: %s",
							rf.ObjType, key)
						internalCacheEntry := rf.GetInternalCacheEntry(obj)
						if err := rf.DeleteResource(rf, obj, internalCacheEntry); err != nil {
							klog.Errorf("Failed to delete old object %s of type %s,"+
								" during add event: %v", key, rf.ObjType, err)
							rf.RecordErrorEvent(obj, err)
							rf.increaseFailedAttemptsCounter(retryObj)
							return
						}
						rf.removeDeleteFromRetryObj(retryObj)
					}
					start := time.Now()
					if err := rf.AddResource(rf, obj, false); err != nil {
						klog.Errorf("Failed to create %s %s, error: %v", rf.ObjType, key, err)
						rf.RecordErrorEvent(obj, err)
						rf.increaseFailedAttemptsCounter(retryObj)
						return
					}
					klog.Infof("Creating %s %s took: %v", rf.ObjType, key, time.Since(start))
					// delete retryObj if handling was successful
					rf.DeleteRetryObj(key)
					rf.RecordSuccessEvent(obj)
				})
			},
			UpdateFunc: func(old, newer interface{}) {
				// skip the whole update if old and newer are equal
				areEqual, err := rf.AreResourcesEqual(old, newer)
				if err != nil {
					klog.Errorf("Could not compare old and newer resource objects of type %s: %v",
						rf.ObjType, err)
					return
				}
				klog.V(5).Infof("Update event received for resource %s, old object is equal to new: %t",
					rf.ObjType, areEqual)
				if areEqual {
					return
				}
				rf.RecordUpdateEvent(newer)

				// get the object keys for newer and old (expected to be the same)
				newKey, err := GetResourceKey(rf.ObjType, newer)
				if err != nil {
					klog.Errorf("Update of %s failed when looking up key of new obj: %v",
						rf.ObjType, err)
					return
				}
				oldKey, err := GetResourceKey(rf.ObjType, old)
				if err != nil {
					klog.Errorf("Update of %s failed when looking up key of old obj: %v",
						rf.ObjType, err)
					return
				}
				if newKey != oldKey {
					klog.Errorf("Could not update resource object of type %s: the key was changed from %s to %s",
						rf.ObjType, oldKey, newKey)
					return
				}

				// skip the whole update if the new object doesn't exist anymore in the API server
				latest, err := rf.GetResourceFromInformerCache(newKey) // TODO just needs access to watchFactory
				if err != nil {
					// When processing an object in terminal state there is a chance that it was already removed from
					// the API server. Since delete events for objects in terminal state are skipped delete it here.
					// This only applies to pod watchers (pods + dynamic network policy handlers watching pods).
					if kerrors.IsNotFound(err) && rf.IsObjectInTerminalState(newer) {
						klog.Warningf("%s %s is in terminal state but no longer exists in informer cache, removing",
							rf.ObjType, newKey)
						rf.processObjectInTerminalState(newer, newKey, resourceEventUpdate)
					} else {
						klog.Warningf("Unable to get %s %s from informer cache (perhaps it was already"+
							" deleted?), skipping update: %v", rf.ObjType, newKey, err)
					}
					return
				}

				klog.V(5).Infof("Update event received for %s %s",
					rf.ObjType, newKey)

				rf.DoWithLock(newKey, func(key string) {
					// STEP 1:
					// Delete existing (old) object if:
					// a) it has a retry entry marked for deletion and doesn't use update or
					// b) the resource is in terminal state (e.g. pod is completed) or
					// c) this resource type has no update function, so an update means delete old obj and add new one
					//
					retryEntryOrNil, found := rf.getRetryObj(key)
					// retryEntryOrNil may be nil if found=false

					if found && retryEntryOrNil.oldObj != nil {
						// [step 1a] there is a retry entry marked for deletion
						klog.Infof("Found retry entry for %s %s marked for deletion: will delete the object",
							rf.ObjType, oldKey)
						if err := rf.DeleteResource(rf, retryEntryOrNil.oldObj,
							retryEntryOrNil.config); err != nil {
							klog.Errorf("Failed to delete stale object %s, during update: %v", oldKey, err)
							rf.RecordErrorEvent(retryEntryOrNil.oldObj, err)
							retryEntry := rf.initRetryObjWithAdd(latest, key)
							rf.increaseFailedAttemptsCounter(retryEntry)
							return
						}
						// remove the old object from retry entry since it was correctly deleted
						if found {
							rf.removeDeleteFromRetryObj(retryEntryOrNil)
						}
					} else if rf.IsObjectInTerminalState(latest) { // check the latest status on newer
						// [step 1b] The object is in a terminal state: delete it from the cluster,
						// delete its retry entry and return. This only applies to pod watchers
						// (pods + dynamic network policy handlers watching pods).
						rf.processObjectInTerminalState(latest, key, resourceEventUpdate)
						return

					} else if !rf.HasUpdateFunc {
						// [step 1c] if this resource type has no update function,
						// delete old obj and in step 2 add the new one
						var existingCacheEntry interface{}
						if found {
							existingCacheEntry = retryEntryOrNil.config
						}
						klog.Infof("Deleting old %s of type %s during update", oldKey, rf.ObjType)
						if err := rf.DeleteResource(rf, old, existingCacheEntry); err != nil {
							klog.Errorf("Failed to delete %s %s, during update: %v",
								rf.ObjType, oldKey, err)
							rf.RecordErrorEvent(old, err)
							retryEntry := rf.InitRetryObjWithDelete(old, key, nil, false)
							rf.initRetryObjWithAdd(latest, key)
							rf.increaseFailedAttemptsCounter(retryEntry)
							return
						}
						// remove the old object from retry entry since it was correctly deleted
						if found {
							rf.removeDeleteFromRetryObj(retryEntryOrNil)
						}
					}
					// STEP 2:
					// Execute the update function for this resource type; resort to add if no update
					// function is available.
					if rf.HasUpdateFunc {
						// if this resource type has an update func, just call the update function
						if err := rf.UpdateResource(rf, old, latest, found); err != nil {
							klog.Errorf("Failed to update %s, old=%s, new=%s, error: %v",
								rf.ObjType, oldKey, newKey, err)
							rf.RecordErrorEvent(latest, err)
							var retryEntry *retryObjEntry
							if rf.ResourceNeedsUpdate() {
								retryEntry = rf.initRetryObjWithUpdate(old, latest, key)
							} else {
								retryEntry = rf.initRetryObjWithAdd(latest, key)
							}
							rf.increaseFailedAttemptsCounter(retryEntry)
							return
						}
					} else { // we previously deleted old object, now let's add the new one
						if err := rf.AddResource(rf, latest, false); err != nil {
							rf.RecordErrorEvent(latest, err)
							retryEntry := rf.initRetryObjWithAdd(latest, key)
							rf.increaseFailedAttemptsCounter(retryEntry)
							klog.Errorf("Failed to add %s %s, during update: %v",
								rf.ObjType, newKey, err)
							return
						}
					}
					rf.DeleteRetryObj(key)
					rf.RecordSuccessEvent(latest)
				})
			},
			DeleteFunc: func(obj interface{}) {
				rf.RecordDeleteEvent(obj)
				key, err := GetResourceKey(rf.ObjType, obj)
				if err != nil {
					klog.Errorf("Delete of %s failed: %v", rf.ObjType, err)
					return
				}
				klog.V(5).Infof("Delete event received for %s %s", rf.ObjType, key)
				// If object is in terminal state, we would have already deleted it during update.
				// No reason to attempt to delete it here again.
				if rf.IsObjectInTerminalState(obj) {
					klog.Infof("Ignoring delete event for resource in terminal state %s %s",
						rf.ObjType, key)
					return
				}
				rf.DoWithLock(key, func(key string) {
					internalCacheEntry := rf.GetInternalCacheEntry(obj)
					retryEntry := rf.InitRetryObjWithDelete(obj, key, internalCacheEntry, false) // set up the retry obj for deletion
					if err = rf.DeleteResource(rf, obj, internalCacheEntry); err != nil {
						retryEntry.failedAttempts++
						klog.Errorf("Failed to delete %s %s, error: %v", rf.ObjType, key, err)
						return
					}
					rf.DeleteRetryObj(key)
					rf.RecordSuccessEvent(obj)
				})
			},
		},
		rf.SyncFunc) // adds all existing objects at startup

	if err != nil {
		return nil, fmt.Errorf("watchResource for resource %v. "+
			"Failed addHandlerFunc: %v", rf.ObjType, err)
	}

	// track the retry entries and every 30 seconds (or upon explicit request) check if any objects
	// need to be retried
	go rf.periodicallyRetryResources()

	return handler, nil
}
