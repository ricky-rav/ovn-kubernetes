package ovn

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	kerrorsutil "k8s.io/apimachinery/pkg/util/errors"

	"k8s.io/klog/v2"

	factory "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

const retryObjInterval = 30 * time.Second

// retryObjEntry is a generic object caching with retry mechanism
//that resources can use to eventually complete their intended operations.
type retryObjEntry struct {
	newObj     interface{}
	oldObj     interface{}
	config     interface{}
	timeStamp  time.Time
	backoffSec time.Duration
	// whether to include this object in the retry iterations
	ignore bool
}

// initRetryObjWithAdd tracks an object that failed to be created to potentially retry later
// initially it is marked as skipped for retry loop (ignore = true)
func (r *retryObjs) initRetryObjWithAdd(obj interface{}, key string) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.timeStamp = time.Now()
		entry.newObj = obj
	} else {
		r.entries[key] = &retryObjEntry{newObj: obj,
			timeStamp: time.Now(), backoffSec: 1, ignore: true}
	}
}

// initRetryWithDelete tracks an object that failed to be deleted to potentially retry later
// initially it is marked as skipped for retry loop (ignore = true)
func (r *retryObjs) initRetryObjWithDelete(obj interface{}, key string, config interface{}) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.timeStamp = time.Now()
		entry.oldObj = obj
		if entry.config == nil {
			entry.config = config
		}
	} else {
		r.entries[key] = &retryObjEntry{oldObj: obj, config: config,
			timeStamp: time.Now(), backoffSec: 1, ignore: true}
	}
}

// addDeleteToRetryObj adds an old object that needs to be cleaned up to a retry object
// includes the config object as well in case the namespace is removed and the object is orphaned from
// the namespace
func (r *retryObjs) addDeleteToRetryObj(obj interface{}, key string, config interface{}) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.oldObj = obj
		entry.config = config
	}
}

// removeDeleteFromRetryObj removes any old object from a retry entry
func (r *retryObjs) removeDeleteFromRetryObj(key string) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.oldObj = nil
		entry.config = nil
	}
}

// unSkipRetryObj ensures an obj is no longer ignored for retry loop
func (r *retryObjs) unSkipRetryObj(key string) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.ignore = false
	}
}

// checkAndDeleteRetryObj deletes a specific entry from the map
func (r *retryObjs) checkAndDeleteRetryObj(key string, withLock bool) {
	if withLock {
		r.retryMutex.Lock()
		defer r.retryMutex.Unlock()
	}
	delete(r.entries, key)
}

// checkAndSkipRetryObj sets a specific entry from the map to be ignored for subsequent retries
func (r *retryObjs) checkAndSkipRetryObj(key string) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.ignore = true
	}
}

// requestRetryObjs allows a caller to immediately request to iterate through all objects that
// are in the retry cache. This will ignore any outstanding time wait/backoff state
func (r *retryObjs) requestRetryObjs() {
	select {
	case r.retryChan <- struct{}{}:
		klog.V(5).Infof("Iterate retry object requested")
	default:
		klog.V(5).Infof("Iterate retry object already requested")
	}
}

//getObjRetryEntry returns a copy of an object  retry entry from the cache
func (r *retryObjs) getObjRetryEntry(key string) *retryObjEntry {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		x := *entry
		return &x
	}
	return nil
}

// triggerRetryObjs track the retry objects map and every 30 seconds check if any object need to be retried
func (oc *Controller) triggerRetryObjs(retryChan chan struct{}, iterateObjs func(bool)) {
	go func() {
		for {
			select {
			case <-time.After(retryObjInterval):
				iterateObjs(false)
			case <-retryChan:
				iterateObjs(true)
			case <-oc.stopChan:
				return
			}
		}
	}()
}

func splitNamespacedName(namespacedName string) (string, string) {
	sep := "/"
	if strings.Contains(namespacedName, sep) {
		s := strings.SplitN(namespacedName, sep, 2)
		if len(s) == 2 {
			return s[0], s[1]
		}
	}
	return namespacedName, ""

}

func getNamespacedName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func HasResourceAnUpdateFunc(objType reflect.Type) bool {
	switch objType {
	case factory.NodeType,
		factory.PeerPodSelectorTypeVar,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorTypeVar:
		// factory.PeerNamespaceAndPodSelectorTypeVar // has no update code
		// factory.PeerNamespaceSelectorTypeVar // has no update code
		return true
	}
	return false
}

func areResourceObjectsEqual(objType reflect.Type, obj1, obj2 interface{}) (bool, error) {
	// switch based on type
	switch objType {
	case factory.PolicyType:
		NP1, ok := obj1.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type interface{} to *knet.NetworkPolicy")
		}
		NP2, ok := obj2.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type interface{}  to *knet.NetworkPolicy")
		}
		return reflect.DeepEqual(NP1, NP2), nil

	case factory.NodeType:
		node1, ok := obj1.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type interface{} to *kapi.Node")
		}
		node2, ok := obj2.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type interface{} to *kapi.Node")
		}

		// when shouldUpdate is false, the hostsubnet is not assigned by ovn-kubernetes
		shouldUpdate, err := shouldUpdate(node2, node1)
		if err != nil {
			klog.Errorf(err.Error())
		}
		return !shouldUpdate, nil

	case factory.PeerServiceTypeVar:
		service1, ok := obj1.(*kapi.Service)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type interface{} to *kapi.Service")
		}
		service2, ok := obj2.(*kapi.Service)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type interface{} to *kapi.Service")
		}
		areEqual := reflect.DeepEqual(service1.Spec.ExternalIPs, service2.Spec.ExternalIPs) &&
			reflect.DeepEqual(service1.Spec.ClusterIP, service2.Spec.ClusterIP) &&
			reflect.DeepEqual(service1.Spec.ClusterIPs, service2.Spec.ClusterIPs) &&
			reflect.DeepEqual(service1.Spec.Type, service2.Spec.Type) &&
			reflect.DeepEqual(service1.Status.LoadBalancer.Ingress, service2.Status.LoadBalancer.Ingress)
		return areEqual, nil

	case factory.PeerPodSelectorTypeVar,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorTypeVar:
		// For these types, there was no old vs new obj comparison in the original update code,
		// so pretend they're always different so that the update code gets executed
		return false, nil

	case factory.PeerNamespaceSelectorTypeVar,
		factory.PeerNamespaceAndPodSelectorTypeVar:
		// For these types there is no update code, so pretend old and new
		// objs are always equivalent and stop processing update event.
		return true, nil
	}

	return false, fmt.Errorf("no object comparison for type %v", objType)
}

func GetResourceObjectKey(objType reflect.Type, obj interface{}) (string, error) {
	switch objType {
	case factory.PolicyType:
		NP, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return "", fmt.Errorf("could not cast interface{} object to *knet.NetworkPolicy")
		}
		return getPolicyNamespacedName(NP), nil

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return "", fmt.Errorf("could not cast interface{} object to *kapi.Node")
		}
		return node.Name, nil

	case factory.PeerServiceTypeVar:
		service, ok := obj.(*kapi.Service)
		if !ok {
			return "", fmt.Errorf("could not cast interface{} object to *kapi.Service")
		}
		return getNamespacedName(service.Namespace, service.Name), nil

	case factory.PeerPodSelectorTypeVar,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorTypeVar:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return "", fmt.Errorf("could not cast interface{} object to *kapi.Pod")
		}
		return getNamespacedName(pod.Namespace, pod.Name), nil

	case factory.PeerNamespaceAndPodSelectorTypeVar,
		factory.PeerNamespaceSelectorTypeVar:
		namespace, ok := obj.(*kapi.Namespace)
		if !ok {
			return "", fmt.Errorf("could not cast interface{} object to *kapi.Namespace")
		}
		return namespace.Name, nil
	}

	return "", fmt.Errorf("object type %v not supported", objType)
}

func (oc *Controller) GetResourceObjectFromInformerCache(objType reflect.Type, key string) (interface{}, error) {
	var obj interface{}
	var err error

	switch objType {
	case factory.PolicyType:
		namespace, name := splitNamespacedName(key)
		obj, err = oc.watchFactory.GetNetworkPolicy(namespace, name)

	case factory.NodeType:
		obj, err = oc.watchFactory.GetNode(key)

	case factory.PeerServiceTypeVar:
		namespace, name := splitNamespacedName(key)
		obj, err = oc.watchFactory.GetService(namespace, name)

	case factory.PeerPodSelectorTypeVar,
		factory.PeerPodForNamespaceAndPodSelectorType:
		namespace, name := splitNamespacedName(key)
		obj, err = oc.watchFactory.GetPod(namespace, name)

	case factory.PeerNamespaceAndPodSelectorTypeVar,
		factory.PeerNamespaceSelectorTypeVar:
		obj, err = oc.watchFactory.GetNamespace(key)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		namespace, name := splitNamespacedName(key)
		obj, err = oc.watchFactory.GetPod(namespace, name)

	default:
		err = fmt.Errorf("object type %v not supported", objType)
	}
	return obj, err
}

func (oc *Controller) addResourceObject(objType reflect.Type, obj, extraParameters interface{}, fromRetryLoop bool) error {
	var err error

	switch objType {
	case factory.PolicyType:
		NP, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast interface{} object to *knet.NetworkPolicy")
		}
		klog.Infof("addResourceObject called on network policy %s/%s",
			NP.Namespace, NP.Name)

		if err = oc.addNetworkPolicy(NP); err != nil {
			klog.Infof("Network Policy retry delete failed for %s/%s, will try again later: %v",
				NP.Namespace, NP.Name, err)
			return err
		}

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast interface{} object to *kapi.Node")
		}
		var nodeParams *nodeSyncs
		if fromRetryLoop {
			_, nodeSync := oc.addNodeFailed.Load(node.Name)
			_, clusterRtrSync := oc.nodeClusterRouterPortFailed.Load(node.Name)
			_, mgmtSync := oc.mgmtPortFailed.Load(node.Name)
			_, gwSync := oc.gatewaysFailed.Load(node.Name)
			nodeParams = &nodeSyncs{
				nodeSync,
				clusterRtrSync,
				mgmtSync,
				gwSync}
		} else {
			nodeParams = &nodeSyncs{true, true, true, true}
		}

		if err = oc.addUpdateNodeEvent(node, nodeParams); err != nil {
			klog.Infof("Node retry delete failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}

	case factory.PeerServiceTypeVar:
		service, ok := obj.(*kapi.Service)
		klog.Infof("addResourceObject called on peer service %s", service.Name)
		if !ok {
			return fmt.Errorf("could not cast peer service of type interface{} to *kapi.Service")
		}
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		return oc.handlePeerServiceAdd(extraParameters.gp, service)

	case factory.PeerPodSelectorTypeVar:
		klog.Infof("addResourceObject called on peer pod")
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorTypeVar:
		klog.Infof("addResourceObject called on PeerNamespaceAndPod")
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		namespace := obj.(*kapi.Namespace)
		extraParameters.np.RLock()
		alreadyDeleted := extraParameters.np.deleted
		extraParameters.np.RUnlock()
		if alreadyDeleted {
			return nil
		}
		klog.Infof("addResourceObject called on PeerNamespaceAndPod, namespace=%s", namespace.Name)

		retryPeerPods := &retryObjs{
			retryMutex:                      sync.Mutex{},
			entries:                         make(map[string]*retryObjEntry),
			retryChan:                       make(chan struct{}, 1),
			oType:                           factory.PeerPodForNamespaceAndPodSelectorType,
			namespaceForFilteredHandler:     namespace.Name,
			labelSelectorForFilteredHandler: extraParameters.podSelector,
			extraParameters: &NetworkPolicyExtraParameters{
				gp: extraParameters.gp},
		}

		// The AddFilteredPodHandler call might call handlePeerPodSelectorAddUpdate
		// on existing pods so we can't be holding the lock at this point
		podHandler := oc.WatchResource(retryPeerPods)

		extraParameters.np.Lock()
		defer extraParameters.np.Unlock()
		if extraParameters.np.deleted {
			oc.watchFactory.RemovePodHandler(podHandler)
			return nil
		}
		extraParameters.np.podHandlerList = append(extraParameters.np.podHandlerList, podHandler)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorTypeVar:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		namespace := obj.(*kapi.Namespace)
		// Update the ACL ...
		return oc.handlePeerNamespaceSelectorOnUpdate(extraParameters.np, extraParameters.gp, func() bool {
			// ... on condition that the added address set was not already in the 'gress policy
			return extraParameters.gp.addNamespaceAddressSet(namespace.Name)
		})

	case factory.LocalPodSelectorTypeVar:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		return oc.handleLocalPodSelectorAddFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			obj)

	default:
		return fmt.Errorf("no add function for object type %v", objType)
	}

	return nil
}

func (oc *Controller) updateResourceObject(objType reflect.Type, oldObj, newObj, extraParameters interface{}) error {
	if hasUpdateFunc := HasResourceAnUpdateFunc(objType); !hasUpdateFunc {
		return fmt.Errorf("no update function for resource type %v", objType)
	}

	switch objType {
	case factory.NodeType:
		newNode, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast newObj of type interface{} to *kapi.Node")
		}
		oldNode, ok := oldObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast oldObj of type interface{} to *kapi.Node")
		}

		// determine what actually changed in this update
		_, nodeSync := oc.addNodeFailed.Load(newNode.Name)
		_, failed := oc.nodeClusterRouterPortFailed.Load(newNode.Name)
		clusterRtrSync := failed || nodeChassisChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)
		_, failed = oc.mgmtPortFailed.Load(newNode.Name)
		mgmtSync := failed || macAddressChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)
		_, failed = oc.gatewaysFailed.Load(newNode.Name)
		gwSync := (failed || gatewayChanged(oldNode, newNode) ||
			nodeSubnetChanged(oldNode, newNode) || hostAddressesChanged(oldNode, newNode))

		return oc.addUpdateNodeEvent(newNode, &nodeSyncs{nodeSync, clusterRtrSync, mgmtSync, gwSync})

	case factory.PeerPodSelectorTypeVar:
		newPod := newObj.(*kapi.Pod)
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}

		if util.PodCompleted(newPod) {
			return oc.handlePeerPodSelectorDelete(extraParameters.gp, oldObj)
		}

		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, newObj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		// TODO Contrary to what we do for PeerPodSelectorTypeVar, here we
		// consider the old object. Is this on purpose?
		oldPod := oldObj.(*kapi.Pod)
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		if util.PodCompleted(oldPod) {
			return oc.handlePeerPodSelectorDelete(extraParameters.gp, oldObj)
		}

		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, newObj)

	case factory.LocalPodSelectorTypeVar:
		newPod := newObj.(*kapi.Pod)
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		if util.PodCompleted(newPod) {
			return oc.handleLocalPodSelectorDelFunc(
				extraParameters.policy,
				extraParameters.np,
				extraParameters.portGroupIngressDenyName,
				extraParameters.portGroupEgressDenyName,
				oldObj)
		}

		return oc.handleLocalPodSelectorAddFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			newObj)
	}

	return fmt.Errorf("no update function for object type %v", objType)
}

func (oc *Controller) deleteResourceObject(objType reflect.Type, kObj, cachedObj, extraParameters interface{}) error {
	klog.Infof("deleteResourceObject called on objectType %v", objType)

	switch objType {
	case factory.PolicyType:
		var cachedNP *networkPolicy
		NP, ok := kObj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast obj of type interface{} to *knet.NetworkPolicy")
		}
		if cachedObj != nil {
			if cachedNP, ok = cachedObj.(*networkPolicy); !ok {
				cachedNP = nil
			}
		}
		klog.Infof("deleteResourceObject called on network policy %s/%s", NP.Namespace, NP.Name)
		if err := oc.deleteNetworkPolicy(NP, cachedNP); err != nil {
			klog.Infof("Network Policy retry delete "+
				"failed for %s/%s, will try again later: %v",
				NP.Namespace, NP.Name, err)
			return err
		}

	case factory.NodeType:
		node, ok := kObj.(*kapi.Node)
		klog.Infof("deleteResourceObject called on node %s", node.Name)

		if !ok {
			return fmt.Errorf("could not cast obj of type interface{} to *knet.Node")
		}
		if err := oc.deleteNodeEvent(node); err != nil {
			klog.Infof("Node retry delete failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}

	case factory.PeerServiceTypeVar:
		service, ok := kObj.(*kapi.Service)
		klog.Infof("deleteResourceObject called on peer service %s", service.Name)
		if !ok {
			return fmt.Errorf("could not cast peer service of type interface{} to *kapi.Service")
		}
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		if err := oc.handlePeerServiceDelete(extraParameters.gp, service); err != nil {
			klog.Infof("Peer service retry delete failed for %s, will try again later: %v",
				service.Name, err)
			return err
		}

	case factory.PeerPodSelectorTypeVar:
		// TODO once you're done debugging, you can remove the casting here below
		// and call handlePeerPodSelectorDelete with kObj
		pod, ok := kObj.(*kapi.Pod)
		klog.Infof("deleteResourceObject called on peer pod %s", pod.Name)
		if !ok {
			return fmt.Errorf("could not cast peer pod of type interface{} to *kapi.Pod")
		}
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		if err := oc.handlePeerPodSelectorDelete(extraParameters.gp, pod); err != nil {
			klog.Infof("Peer pod retry delete failed for %s, will try again later: %v",
				pod.Name, err)
			return err
		}

	case factory.PeerNamespaceAndPodSelectorTypeVar:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		// when the namespace labels no longer apply
		// remove the namespaces pods from the address_set
		var errs []error
		namespace := kObj.(*kapi.Namespace)
		pods, _ := oc.watchFactory.GetPods(namespace.Name)

		for _, pod := range pods {
			if err := oc.handlePeerPodSelectorDelete(extraParameters.gp, pod); err != nil {
				errs = append(errs, err)
			}
		}
		return kerrorsutil.NewAggregate(errs)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		return oc.handlePeerPodSelectorDelete(extraParameters.gp, kObj)

	case factory.PeerNamespaceSelectorTypeVar:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}
		namespace := kObj.(*kapi.Namespace)
		// Remove namespace address set from the *gress policy in cache
		// (done in gress.delNamespaceAddressSet()), and then update ACLs
		return oc.handlePeerNamespaceSelectorOnUpdate(extraParameters.np, extraParameters.gp, func() bool {
			// ... on condition that the removed address set was in the 'gress policy
			return extraParameters.gp.delNamespaceAddressSet(namespace.Name)
		})

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}

		return oc.handleLocalPodSelectorDelFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			kObj)
	case factory.LocalPodSelectorTypeVar:
		extraParameters, ok := extraParameters.(*NetworkPolicyExtraParameters)
		if !ok {
			return fmt.Errorf("could not cast extraParameters to " +
				" *NetworkPolicyExtraParameters")
		}

		return oc.handleLocalPodSelectorDelFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			kObj)

	default:
		return fmt.Errorf("object type %v not supported", objType)
	}

	return nil
}

// iterateRetryResourceObjects checks if any outstanding resource objects exist
// and if so it tries to re-add them.
// updateAll forces all objects to be attempted to be retried regardless.
func (oc *Controller) iterateRetryResourceObjects(r *retryObjs, updateAll bool) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	now := time.Now()
	for objKey, entry := range r.entries {
		if entry.ignore {
			continue
		}
		// check if we need to create the resource object
		var objectToCreate interface{}
		if entry.newObj != nil {
			// get the latest version of the resource object from the informer;
			// if it doesn't exist we are not going to create the new object.
			obj, err := oc.GetResourceObjectFromInformerCache(r.oType, objKey)
			if err != nil && kerrors.IsNotFound(err) {
				klog.Infof("%v %s not found in the informers cache,"+
					" not going to retry object create", r.oType, objKey)
				entry.newObj = nil
			} else { // TODO what if it's any other error?
				objectToCreate = obj
			}
		}

		entry.backoffSec = entry.backoffSec * 2
		if entry.backoffSec > 60 {
			entry.backoffSec = 60
		}
		backoff := (entry.backoffSec * time.Second) + (time.Duration(rand.Intn(500)) * time.Millisecond)
		objTimer := entry.timeStamp.Add(backoff)
		if !updateAll && now.Before(objTimer) {
			klog.Infof("%v retry %s not after timer yet, time: %s",
				r.oType, objKey, objTimer)
			continue
		}

		klog.Infof("%v retry: %s retry object setup", r.oType, objKey)

		// check if we need to delete anything
		if entry.oldObj != nil {
			klog.Infof("%v retry: removing old object for %s", r.oType, objKey)
			if err := oc.deleteResourceObject(r.oType, entry.oldObj,
				entry.config, r.extraParameters); err != nil {
				klog.Infof("%v retry delete failed for %s, will try again later: %v",
					r.oType, objKey, err)
				entry.timeStamp = time.Now()
				continue
			}
			// successfully cleaned up old object, remove it from the retry cache
			entry.oldObj = nil
		}

		// create new object if needed
		if objectToCreate != nil {
			klog.Infof("%v retry: Creating object for %s",
				r.oType, objKey)
			if err := oc.addResourceObject(r.oType, objectToCreate,
				r.extraParameters, true); err != nil {
				klog.Infof("%v retry create "+
					"failed for %s, will try again later: %v",
					r.oType, objKey, err)
				entry.timeStamp = time.Now()
				continue
			}
			// successfully cleaned up old object, remove it from the retry cache
			entry.newObj = nil
		}

		klog.Infof("%v retry successful for %s", r.oType, objKey)
		r.checkAndDeleteRetryObj(objKey, false)

	}
}

func (oc *Controller) periodicallyRetryResourceObjects(r *retryObjs) {
	// track the retryObjs map and every 30 seconds check if any object needs to be retried
	for {
		select {
		case <-time.After(retryObjInterval):
			klog.V(5).Infof("%s s have elapsed, retrying failed objects of type %v", r.oType)
			oc.iterateRetryResourceObjects(r, false)

		case <-r.retryChan:
			klog.V(5).Infof("Retry channel got triggered: retrying failed objects of type %v", r.oType)
			oc.iterateRetryResourceObjects(r, true)

		case <-oc.stopChan:
			klog.V(5).Infof("Stop channel got triggered: will stop retrying failed objects of type %v", r.oType)
			return
		}
	}
}

func (oc *Controller) getSyncResourceObjectsFunc(
	objType reflect.Type,
	inputSyncFunc func([]interface{}) error,
	extraParameters interface{}) (func([]interface{}), error) {

	var syncRetriableFunc func([]interface{}) error
	var syncFunc func([]interface{})

	var name string
	// If a type needs a retriable sync funcion, it will set syncRetriableFunc
	// and will keep syncFunc=nil. Otherwise, it will directly set syncFunc.
	switch objType {
	case factory.PolicyType:
		name = "syncNetworkPolicies"
		syncRetriableFunc = oc.syncNetworkPoliciesRetriable

	case factory.NodeType:
		name = "syncNodes"
		syncRetriableFunc = oc.syncNodesRetriable

	case factory.PeerServiceTypeVar,
		factory.PeerNamespaceAndPodSelectorTypeVar:
		name = ""
		syncRetriableFunc = nil

	case factory.PeerPodSelectorTypeVar:
		name = "PeerPodSelector"
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		syncRetriableFunc = func(objs []interface{}) error {
			return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, objs...)
		}

	case factory.PeerPodForNamespaceAndPodSelectorType:
		name = "PeerPodForNamespaceAndPodSelector"
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		syncRetriableFunc = func(objs []interface{}) error {
			return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, objs...)
		}

	case factory.PeerNamespaceSelectorTypeVar:
		name = "PeerNamespaceSelector"
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		// the function below will never fail, so there's no point in making it retriable...
		syncFunc = func(i []interface{}) {
			// This needs to be a write lock because there's no locking around 'gress policies
			extraParameters.np.Lock()
			defer extraParameters.np.Unlock()
			// We load the existing address set into the 'gress policy.
			// Notice that this will make the AddFunc for this initial
			// address set a noop.
			// The ACL must be set explicitly after setting up this handler
			// for the address set to be considered.
			extraParameters.gp.addNamespaceAddressSets(i)
		}

	case factory.LocalPodSelectorTypeVar:
		name = "LocalPodSelectorTypeVar"
		syncRetriableFunc = inputSyncFunc

	default:
		return nil, fmt.Errorf("no sync function for object type %v", objType)
	}

	if syncFunc == nil {
		syncFunc = func(objects []interface{}) {
			if syncRetriableFunc == nil {
				return
			}
			oc.syncWithRetry(name, func() error { return syncRetriableFunc(objects) })
		}
	}
	return syncFunc, nil
}
