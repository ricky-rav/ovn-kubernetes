package ovn

import (
	"fmt"
	"net"
	"reflect"

	ocpcloudnetworkapi "github.com/openshift/api/cloudnetwork/v1"

	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/labels"
	kerrorsutil "k8s.io/apimachinery/pkg/util/errors"
	cache "k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"

	egressfirewall "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1"
	egressipv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	factory "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	retry "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

func (oc *Controller) newRetryFrameworkMaster(
	objectType reflect.Type,
	namespaceForFilteredHandler string,
	labelSelectorForFilteredHandler labels.Selector,
	syncFunc func([]interface{}) error,
	extraParameters interface{}) *retry.RetryFramework {

	rf := retry.NewRetryFramework(
		objectType,
		namespaceForFilteredHandler,
		labelSelectorForFilteredHandler,
		syncFunc,
		extraParameters,
		oc.stopChan,
		oc.watchFactory)

	// override resource-specific functions
	rf.HasUpdateFunc = hasResourceAnUpdateFunc(rf.ObjType)

	rf.AddResource = oc.addResource
	rf.UpdateResource = oc.updateResource
	rf.DeleteResource = oc.deleteResource

	// what should we do with errors??? discard them for now
	// I prefer an init function per resource, once the poc is validated
	var err error
	if syncFunc != nil {
		rf.SyncFunc = syncFunc
	} else {
		if rf.SyncFunc, err = oc.getSyncResourcesFunc(rf); err != nil {
			// maybe return an error?
			klog.Errorf("Could not get syncFunc for resource type %s, won't create retry framework for it")
			return nil
		}
	}

	rf.GetResourceFromInformerCache = func(key string) (interface{}, error) {
		return oc.getResourceFromInformerCache(rf.ObjType, key)
	}

	rf.AreResourcesEqual = func(obj1, obj2 interface{}) (bool, error) {
		return areResourcesEqual(rf.ObjType, obj1, obj2)
	}
	rf.GetInternalCacheEntry = func(obj interface{}) interface{} {
		return oc.getInternalCacheEntry(rf.ObjType, obj)
	}
	rf.IsResourceScheduled = func(obj interface{}) bool {
		return isResourceScheduled(rf.ObjType, obj)
	}
	rf.ResourceNeedsUpdate = func() bool {
		return resourceNeedsUpdate(rf.ObjType)
	}
	rf.IsObjectInTerminalState = func(obj interface{}) bool {
		return isObjectInTerminalState(objectType, obj)
	}
	rf.RecordAddEvent = func(obj interface{}) {
		oc.recordAddEvent(rf.ObjType, obj)
	}
	rf.RecordUpdateEvent = func(obj interface{}) {
		recordUpdateEvent(rf.ObjType, obj)
	}

	rf.RecordDeleteEvent = func(obj interface{}) {
		oc.recordDeleteEvent(rf.ObjType, obj)
	}

	rf.RecordSuccessEvent = func(obj interface{}) {
		recordSuccessEvent(rf.ObjType, obj)
	}

	rf.RecordErrorEvent = func(obj interface{}, err error) {
		oc.recordErrorEvent(rf.ObjType, obj, err)
	}

	klog.Infof("Done initializing retry framework for %s", objectType)
	return rf
}

// TODO
// Maybe addNewRetryFrameworkMasterWithFilters for network policies?

// NewRetryFramework returns a new RetryFramework instance, packed with the desired input parameters.
// The returned struct is essential for watchResource and the whole retry logic.
// (oc *Controller) func NewRetryFrameworkMaster(
// 	objectType reflect.Type,
// 	namespaceForFilteredHandler string,
// 	labelSelectorForFilteredHandler labels.Selector,
// 	syncFunc func([]interface{}) error,
// 	extraParameters interface{}) *retry.RetryFramework {

// 	switch rf.ObjType {
// 	case factory.PodType:
// 		return retry.NewRetryFramework(factory.PodType, "", nil, nil, nil)

// 	case factory.PolicyType:
// 		return retry.NewRetryFramework(factory.PolicyType, "", nil, nil, nil)

// 	case factory.NodeType:
// 		return retry.NewRetryFramework(factory.NodeType, "", nil, nil, nil)

// 	case factory.PeerServiceType,

// 	case factory.PeerPodSelectorType:

// 	case factory.PeerPodForNamespaceAndPodSelectorType:

// 	case factory.PeerNamespaceSelectorType:

// 	case factory.LocalPodSelectorType:

// 	case factory.EgressFirewallType:
// 		return retry.NewRetryFramework(factory.EgressFirewallType, "", nil, nil, nil)

// 	case factory.EgressIPType:
// 		return retry.NewRetryFramework(factory.EgressIPType, "", nil, nil, nil)

// 	case factory.EgressIPNamespaceType:
// 		return retry.NewRetryFramework(factory.EgressIPNamespaceType, "", nil, nil, nil)

// 	case factory.EgressIPPodType:
// 		return retry.NewRetryFramework(factory.EgressIPPodType, "", nil, nil, nil)

// 	case factory.EgressNodeType:
// 		return retry.NewRetryFramework(factory.EgressNodeType, "", nil, nil, nil)

// 	case factory.CloudPrivateIPConfigType:
// 		return retry.NewRetryFramework(factory.CloudPrivateIPConfigType, "", nil, nil, nil)

// 	default:
// 		return nil
// 	}

// 	// return &RetryFramework{
// 	// 	retryEntries:                    syncmap.NewSyncMap[*retryObjEntry](),
// 	// 	retryChan:                       make(chan struct{}, 1),
// 	// 	ObjType:                           objectType,
// 	// 	namespaceForFilteredHandler:     namespaceForFilteredHandler,
// 	// 	labelSelectorForFilteredHandler: labelSelectorForFilteredHandler,
// 	// 	syncFunc:                        syncFunc,
// 	// 	extraParameters:                 extraParameters,
// 	// }
// }

// hasResourceAnUpdateFunc returns true if the given resource type has a dedicated update function.
// It returns false if, upon an update event on this resource type, we instead need to first delete the old
// object and then add the new one.
func hasResourceAnUpdateFunc(objType reflect.Type) bool {
	switch objType {
	case factory.PodType,
		factory.NodeType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.EgressIPType,
		factory.EgressIPNamespaceType,
		factory.EgressIPPodType,
		factory.EgressNodeType,
		factory.CloudPrivateIPConfigType,
		factory.LocalPodSelectorType:
		return true
	}
	return false
}

// areResourcesEqual returns true if, given two objects of a known resource type, the update logic for this resource
// type considers them equal and therefore no update is needed. It returns false when the two objects are not considered
// equal and an update needs be executed. This is regardless of how the update is carried out (whether with a dedicated update
// function or with a delete on the old obj followed by an add on the new obj).
func areResourcesEqual(objType reflect.Type, obj1, obj2 interface{}) (bool, error) {
	// switch based on type
	switch objType {
	case factory.PolicyType:
		np1, ok := obj1.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *knet.NetworkPolicy", obj1)
		}
		np2, ok := obj2.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *knet.NetworkPolicy", obj2)
		}
		return reflect.DeepEqual(np1, np2), nil

	case factory.NodeType:
		node1, ok := obj1.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *kapi.Node", obj1)
		}
		node2, ok := obj2.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *kapi.Node", obj2)
		}

		// when shouldUpdate is false, the hostsubnet is not assigned by ovn-kubernetes
		shouldUpdate, err := shouldUpdate(node2, node1) // TODO this should be passed as input when initializing retry for nodes
		if err != nil {
			klog.Errorf(err.Error())
		}
		return !shouldUpdate, nil

	case factory.PeerServiceType:
		service1, ok := obj1.(*kapi.Service)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *kapi.Service", obj1)
		}
		service2, ok := obj2.(*kapi.Service)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *kapi.Service", obj2)
		}
		areEqual := reflect.DeepEqual(service1.Spec.ExternalIPs, service2.Spec.ExternalIPs) &&
			reflect.DeepEqual(service1.Spec.ClusterIP, service2.Spec.ClusterIP) &&
			reflect.DeepEqual(service1.Spec.ClusterIPs, service2.Spec.ClusterIPs) &&
			reflect.DeepEqual(service1.Spec.Type, service2.Spec.Type) &&
			reflect.DeepEqual(service1.Status.LoadBalancer.Ingress, service2.Status.LoadBalancer.Ingress)
		return areEqual, nil

	case factory.PodType,
		factory.EgressIPPodType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorType:
		// For these types, there was no old vs new obj comparison in the original update code,
		// so pretend they're always different so that the update code gets executed
		return false, nil

	case factory.PeerNamespaceSelectorType,
		factory.PeerNamespaceAndPodSelectorType:
		// For these types there is no update code, so pretend old and new
		// objs are always equivalent and stop processing the update event.
		return true, nil

	case factory.EgressFirewallType:
		oldEgressFirewall, ok := obj1.(*egressfirewall.EgressFirewall)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *egressfirewall.EgressFirewall", obj1)
		}
		newEgressFirewall, ok := obj2.(*egressfirewall.EgressFirewall)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *egressfirewall.EgressFirewall", obj2)
		}
		return reflect.DeepEqual(oldEgressFirewall.Spec, newEgressFirewall.Spec), nil

	case factory.EgressIPType,
		factory.EgressIPNamespaceType,
		factory.EgressNodeType,
		factory.CloudPrivateIPConfigType:
		// force update path for EgressIP resource.
		return false, nil

	}

	return false, fmt.Errorf("no object comparison for type %s", objType)
}

// // Given an object and its type, it returns the key for this object and an error if the key retrieval failed.
// // For all namespaced resources, the key will be namespace/name. For resource types without a namespace,
// // the key will be the object name itself.
// func GetResourceKey(objType reflect.Type, obj interface{}) (string, error) {

// 	return cache.MetaNamespaceKeyFunc(obj)

// 	// switch objType {
// 	// case factory.PolicyType:
// 	// 	np, ok := obj.(*knet.NetworkPolicy)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *knet.NetworkPolicy", obj)
// 	// 	}
// 	// 	return getPolicyNamespacedName(np), nil

// 	// case factory.NodeType,
// 	// 	factory.EgressNodeType:
// 	// 	node, ok := obj.(*kapi.Node)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Node", obj)
// 	// 	}
// 	// 	return node.Name, nil

// 	// case factory.PeerServiceType:
// 	// 	service, ok := obj.(*kapi.Service)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Service", obj)
// 	// 	}
// 	// 	return getNamespacedName(service.Namespace, service.Name), nil

// 	// case factory.PodType,
// 	// 	factory.PeerPodSelectorType,
// 	// 	factory.PeerPodForNamespaceAndPodSelectorType,
// 	// 	factory.LocalPodSelectorType,
// 	// 	factory.EgressIPPodType:
// 	// 	pod, ok := obj.(*kapi.Pod)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Pod", obj)
// 	// 	}
// 	// 	return getNamespacedName(pod.Namespace, pod.Name), nil

// 	// case factory.PeerNamespaceAndPodSelectorType,
// 	// 	factory.PeerNamespaceSelectorType,
// 	// 	factory.EgressIPNamespaceType:
// 	// 	namespace, ok := obj.(*kapi.Namespace)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *kapi.Namespace", obj)
// 	// 	}
// 	// 	return namespace.Name, nil

// 	// case factory.EgressFirewallType:
// 	// 	egressFirewall, ok := obj.(*egressfirewall.EgressFirewall)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *egressfirewall.EgressFirewall", obj)
// 	// 	}
// 	// 	return getEgressFirewallNamespacedName(egressFirewall), nil

// 	// case factory.EgressIPType:
// 	// 	eIP, ok := obj.(*egressipv1.EgressIP)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *egressipv1.EgressIP", obj)
// 	// 	}
// 	// 	return eIP.Name, nil
// 	// case factory.CloudPrivateIPConfigType:
// 	// 	cloudPrivateIPConfig, ok := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
// 	// 	if !ok {
// 	// 		return "", fmt.Errorf("could not cast %T object to *ocpcloudnetworkapi.CloudPrivateIPConfig", obj)
// 	// 	}
// 	// 	return cloudPrivateIPConfig.Name, nil
// 	// }

// 	// return "", fmt.Errorf("object type %s not supported", objType)
// }

// TODO to be moved to obj_retry_master.go
func (oc *Controller) getPortInfo(pod *kapi.Pod) *lpInfo {
	var portInfo *lpInfo
	key := util.GetLogicalPortName(pod.Namespace, pod.Name)
	if !util.PodWantsNetwork(pod) {
		// create dummy logicalPortInfo for host-networked pods
		mac, _ := net.ParseMAC("00:00:00:00:00:00")
		portInfo = &lpInfo{
			logicalSwitch: "host-networked",
			name:          key,
			uuid:          "host-networked",
			ips:           []*net.IPNet{},
			mac:           mac,
		}
	} else {
		portInfo, _ = oc.logicalPortCache.get(key)
	}
	return portInfo
}

// Given an object and its type, getInternalCacheEntry returns the internal cache entry for this object.
// This is now used only for pods, which will get their the logical port cache entry.
// TODO this should be filled in by obj_retry_master.go when initializing retry for ovnk master
// HOW??? Should each resource type create a struct, fill it with some functions, and then in obj_retry.go
// I define generic functions over that struct? Yes
func (oc *Controller) getInternalCacheEntry(objType reflect.Type, obj interface{}) interface{} {
	switch objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		return oc.getPortInfo(pod)
	default:
		return nil
	}
}

// Given an object key and its type, getResourceFromInformerCache returns the latest state of the object
// from the informers cache.
// TODO In the generic pkg, it only needs access to the watchFactory
// TODO Maybe have a global overall retry struct initialized with a watchFactory instance? Then here you'd call only that
// it would have whatever is necessary for both master and ovn, that is an intersection of their controller attributes.
func (oc *Controller) getResourceFromInformerCache(objType reflect.Type, key string) (interface{}, error) {
	var obj interface{}
	var namespace, name string
	var err error

	namespace, name, err = cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to split key %s: %v", key, err)
	}

	switch objType {
	case factory.PolicyType:
		obj, err = oc.watchFactory.GetNetworkPolicy(namespace, name)

	case factory.NodeType,
		factory.EgressNodeType:
		obj, err = oc.watchFactory.GetNode(name)

	case factory.PeerServiceType:
		obj, err = oc.watchFactory.GetService(namespace, name)

	case factory.PodType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorType,
		factory.EgressIPPodType:
		obj, err = oc.watchFactory.GetPod(namespace, name)

	case factory.PeerNamespaceAndPodSelectorType,
		factory.PeerNamespaceSelectorType,
		factory.EgressIPNamespaceType:
		obj, err = oc.watchFactory.GetNamespace(name)

	case factory.EgressFirewallType:
		obj, err = oc.watchFactory.GetEgressFirewall(namespace, name)

	case factory.EgressIPType:
		obj, err = oc.watchFactory.GetEgressIP(name)

	case factory.CloudPrivateIPConfigType:
		obj, err = oc.watchFactory.GetCloudPrivateIPConfig(name)

	default:
		err = fmt.Errorf("object type %s not supported, cannot retrieve it from informers cache",
			objType)
	}
	return obj, err
}

// Given an object and its type, recordAddEvent records the add event on this object.
// TODO in obj_retry_master.go, you need to pass the pod-recorder initialized in master or pass the recordAddEvent function for pods
func (oc *Controller) recordAddEvent(objType reflect.Type, obj interface{}) {
	switch objType {
	case factory.PodType:
		klog.V(5).Infof("Recording add event on pod")
		pod := obj.(*kapi.Pod)
		oc.podRecorder.AddPod(pod.UID)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		klog.V(5).Infof("Recording add event on network policy")
		np := obj.(*knet.NetworkPolicy)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

// Given an object and its type, recordUpdateEvent records the update event on this object.
func recordUpdateEvent(objType reflect.Type, obj interface{}) {
	switch objType {
	case factory.PodType:
		klog.V(5).Infof("Recording update event on pod")
		pod := obj.(*kapi.Pod)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		klog.V(5).Infof("Recording update event on network policy")
		np := obj.(*knet.NetworkPolicy)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

// Given an object and its type, recordDeleteEvent records the delete event on this object. Only used for pods now.
// TODO in obj_retry_master.go, you need to pass the pod-recorder initialized in master or pass the CleanPod function for pods
func (oc *Controller) recordDeleteEvent(objType reflect.Type, obj interface{}) {
	switch objType {
	case factory.PodType:
		klog.V(5).Infof("Recording delete event on pod")
		pod := obj.(*kapi.Pod)
		oc.podRecorder.CleanPod(pod.UID)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		klog.V(5).Infof("Recording delete event on network policy")
		np := obj.(*knet.NetworkPolicy)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

func recordSuccessEvent(objType reflect.Type, obj interface{}) {
	switch objType {
	case factory.PodType:
		klog.V(5).Infof("Recording success event on pod")
		pod := obj.(*kapi.Pod)
		metrics.GetConfigDurationRecorder().End("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		klog.V(5).Infof("Recording success event on network policy")
		np := obj.(*knet.NetworkPolicy)
		metrics.GetConfigDurationRecorder().End("networkpolicy", np.Namespace, np.Name)
	}
}

// Given an object and its type, recordErrorEvent records an error event on this object.
// Only used for pods now.
// // TODO in obj_retry_master.go, you need to pass the snippet below
func (oc *Controller) recordErrorEvent(objType reflect.Type, obj interface{}, err error) {
	switch objType {
	case factory.PodType:
		klog.V(5).Infof("Recording error event on pod")
		pod := obj.(*kapi.Pod)
		oc.recordPodEvent(err, pod)
	}
}

// Given an object and its type, isResourceScheduled returns true if the object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
// TODO ok as is
func isResourceScheduled(objType reflect.Type, obj interface{}) bool {
	switch objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		return util.PodScheduled(pod)
	}
	return true
}

// Given an object type, resourceNeedsUpdate returns true if the object needs to invoke update during iterate retry.
func resourceNeedsUpdate(objType reflect.Type) bool {
	switch objType {
	case factory.EgressNodeType,
		factory.EgressIPType,
		factory.EgressIPPodType,
		factory.EgressIPNamespaceType,
		factory.CloudPrivateIPConfigType:
		return true
	}
	return false
}

// Given a *RetryFramework instance, an object to add and a boolean specifying if the function was executed from
// iterateRetryResources, addResource adds the specified object to the cluster according to its type and
// returns the error, if any, yielded during object creation.
func (oc *Controller) addResource(rf *retry.RetryFramework, obj interface{}, fromRetryLoop bool) error {
	var err error
	klog.Infof("addResource called on  %s", rf.ObjType)
	defer klog.Infof("addResource done on  %s", rf.ObjType)

	switch rf.ObjType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return oc.ensurePod(nil, pod, true)

	case factory.PolicyType:
		np, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.NetworkPolicy", obj)
		}

		if err = oc.addNetworkPolicy(np); err != nil {
			klog.Infof("Network Policy add failed for %s/%s, will try again later: %v",
				np.Namespace, np.Name, err)
			return err
		}

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
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
			klog.Infof("Node add failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}

	case factory.PeerServiceType:
		service, ok := obj.(*kapi.Service)
		if !ok {
			return fmt.Errorf("could not cast peer service of type %T to *kapi.Service", obj)
		}
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerServiceAdd(extraParameters.gp, service)

	case factory.PeerPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		namespace := obj.(*kapi.Namespace)
		extraParameters.np.RLock()
		alreadyDeleted := extraParameters.np.deleted
		extraParameters.np.RUnlock()
		if alreadyDeleted {
			return nil
		}

		// start watching pods in this namespace and selected by the label selector in extraParameters.podSelector
		peerPodsRetryFramework := retry.NewRetryFramework(
			factory.PeerPodForNamespaceAndPodSelectorType,
			namespace.Name,
			extraParameters.podSelector,
			nil,
			&NetworkPolicyExtraParameters{gp: extraParameters.gp},
			oc.stopChan,
			oc.watchFactory,
		)
		// The AddFilteredPodHandler call might call handlePeerPodSelectorAddUpdate
		// on existing pods so we can't be holding the lock at this point
		podHandler, err := peerPodsRetryFramework.WatchResource()
		if err != nil {
			klog.Errorf("Failed WatchResource for PeerNamespaceAndPodSelectorType: %v", err)
			return err
		}

		extraParameters.np.Lock()
		defer extraParameters.np.Unlock()
		if extraParameters.np.deleted {
			oc.watchFactory.RemovePodHandler(podHandler)
			return nil
		}
		extraParameters.np.podHandlerList = append(extraParameters.np.podHandlerList, podHandler)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		namespace := obj.(*kapi.Namespace)
		// Update the ACL ...
		return oc.handlePeerNamespaceSelectorOnUpdate(extraParameters.np, extraParameters.gp, func() bool {
			// ... on condition that the added address set was not already in the 'gress policy
			return extraParameters.gp.addNamespaceAddressSet(namespace.Name)
		})

	case factory.LocalPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handleLocalPodSelectorAddFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			obj)

	case factory.EgressFirewallType:
		var err error
		egressFirewall := obj.(*egressfirewall.EgressFirewall).DeepCopy()
		if err = oc.addEgressFirewall(egressFirewall); err != nil {
			egressFirewall.Status.Status = egressFirewallAddError
		} else {
			egressFirewall.Status.Status = egressFirewallAppliedCorrectly
			metrics.UpdateEgressFirewallRuleCount(float64(len(egressFirewall.Spec.Egress)))
			metrics.IncrementEgressFirewallCount()
		}
		if err := oc.updateEgressFirewallStatusWithRetry(egressFirewall); err != nil {
			klog.Errorf("Failed to update egress firewall status %s, error: %v",
				getEgressFirewallNamespacedName(egressFirewall), err)
		}
		return err

	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return oc.reconcileEgressIP(nil, eIP)

	case factory.EgressIPNamespaceType:
		namespace := obj.(*kapi.Namespace)
		return oc.reconcileEgressIPNamespace(nil, namespace)

	case factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return oc.reconcileEgressIPPod(nil, pod)

	case factory.EgressNodeType:
		node := obj.(*kapi.Node)
		if err := oc.setupNodeForEgress(node); err != nil {
			return err
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		_, hasEgressLabel := nodeLabels[nodeEgressLabel]
		if hasEgressLabel {
			oc.setNodeEgressAssignable(node.Name, true)
		}
		isReady := oc.isEgressNodeReady(node)
		if isReady {
			oc.setNodeEgressReady(node.Name, true)
		}
		isReachable := oc.isEgressNodeReachable(node)
		if isReachable {
			oc.setNodeEgressReachable(node.Name, true)
		}
		if hasEgressLabel && isReachable && isReady {
			if err := oc.addEgressNode(node.Name); err != nil {
				return err
			}
		}

	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return oc.reconcileCloudPrivateIPConfig(nil, cloudPrivateIPConfig)

	default:
		return fmt.Errorf("no add function for object type %s", rf.ObjType)
	}

	return nil
}

// Given a *RetryFramework instance, an old and a new object, updateResource updates the specified object in the cluster
// to its version in newObj according to its type and returns the error, if any, yielded during the object update.
// The inRetryCache boolean argument is to indicate if the given resource is in the retryCache or not.
func (oc *Controller) updateResource(rf *retry.RetryFramework, oldObj, newObj interface{}, inRetryCache bool) error {
	switch rf.ObjType {
	case factory.PodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)

		return oc.ensurePod(oldPod, newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))

	case factory.NodeType:
		newNode, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast newObj of type %T to *kapi.Node", newObj)
		}
		oldNode, ok := oldObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast oldObj of type %T to *kapi.Node", oldObj)
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

	case factory.PeerPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, newObj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, newObj)

	case factory.LocalPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handleLocalPodSelectorAddFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			newObj)

	case factory.EgressIPType:
		oldEIP := oldObj.(*egressipv1.EgressIP)
		newEIP := newObj.(*egressipv1.EgressIP)
		return oc.reconcileEgressIP(oldEIP, newEIP)

	case factory.EgressIPNamespaceType:
		oldNamespace := oldObj.(*kapi.Namespace)
		newNamespace := newObj.(*kapi.Namespace)
		return oc.reconcileEgressIPNamespace(oldNamespace, newNamespace)

	case factory.EgressIPPodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)
		return oc.reconcileEgressIPPod(oldPod, newPod)

	case factory.EgressNodeType:
		oldNode := oldObj.(*kapi.Node)
		newNode := newObj.(*kapi.Node)
		// Initialize the allocator on every update,
		// ovnkube-node/cloud-network-config-controller will make sure to
		// annotate the node with the egressIPConfig, but that might have
		// happened after we processed the ADD for that object, hence keep
		// retrying for all UPDATEs.
		if err := oc.initEgressIPAllocator(newNode); err != nil {
			klog.Warningf("Egress node initialization error: %v", err)
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		oldLabels := oldNode.GetLabels()
		newLabels := newNode.GetLabels()
		_, oldHadEgressLabel := oldLabels[nodeEgressLabel]
		_, newHasEgressLabel := newLabels[nodeEgressLabel]
		// If the node is not labeled for egress assignment, just return
		// directly, we don't really need to set the ready / reachable
		// status on this node if the user doesn't care about using it.
		if !oldHadEgressLabel && !newHasEgressLabel {
			return nil
		}
		if oldHadEgressLabel && !newHasEgressLabel {
			klog.Infof("Node: %s has been un-labeled, deleting it from egress assignment", newNode.Name)
			oc.setNodeEgressAssignable(oldNode.Name, false)
			return oc.deleteEgressNode(oldNode.Name)
		}
		isOldReady := oc.isEgressNodeReady(oldNode)
		isNewReady := oc.isEgressNodeReady(newNode)
		isNewReachable := oc.isEgressNodeReachable(newNode)
		oc.setNodeEgressReady(newNode.Name, isNewReady)
		oc.setNodeEgressReachable(newNode.Name, isNewReachable)
		if !oldHadEgressLabel && newHasEgressLabel {
			klog.Infof("Node: %s has been labeled, adding it for egress assignment", newNode.Name)
			oc.setNodeEgressAssignable(newNode.Name, true)
			if isNewReady && isNewReachable {
				if err := oc.addEgressNode(newNode.Name); err != nil {
					return err
				}
			} else {
				klog.Warningf("Node: %s has been labeled, but node is not ready"+
					" and reachable, cannot use it for egress assignment", newNode.Name)
			}
			return nil
		}
		if isOldReady == isNewReady {
			return nil
		}
		if !isNewReady {
			klog.Warningf("Node: %s is not ready, deleting it from egress assignment", newNode.Name)
			if err := oc.deleteEgressNode(newNode.Name); err != nil {
				return err
			}
		} else if isNewReady && isNewReachable {
			klog.Infof("Node: %s is ready and reachable, adding it for egress assignment", newNode.Name)
			if err := oc.addEgressNode(newNode.Name); err != nil {
				return err
			}
		}
		return nil

	case factory.CloudPrivateIPConfigType:
		oldCloudPrivateIPConfig := oldObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		newCloudPrivateIPConfig := newObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return oc.reconcileCloudPrivateIPConfig(oldCloudPrivateIPConfig, newCloudPrivateIPConfig)
	}

	return fmt.Errorf("no update function for object type %s", rf.ObjType)
}

// Given a *RetryFramework instance, an object and optionally a cachedObj, deleteResource deletes the object from the cluster
// according to the delete logic of its resource type. cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (oc *Controller) deleteResource(rf *retry.RetryFramework, obj, cachedObj interface{}) error {
	switch rf.ObjType {
	case factory.PodType:
		var portInfo *lpInfo
		pod := obj.(*kapi.Pod)

		if cachedObj != nil {
			portInfo = cachedObj.(*lpInfo)
		}
		oc.logicalPortCache.remove(util.GetLogicalPortName(pod.Namespace, pod.Name))
		return oc.removePod(pod, portInfo)

	case factory.PolicyType:
		var cachedNP *networkPolicy
		knp, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.NetworkPolicy", obj)
		}

		if cachedObj != nil {
			if cachedNP, ok = cachedObj.(*networkPolicy); !ok {
				cachedNP = nil
			}
		}
		return oc.deleteNetworkPolicy(knp, cachedNP)

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return oc.deleteNodeEvent(node)

	case factory.PeerServiceType:
		service, ok := obj.(*kapi.Service)
		if !ok {
			return fmt.Errorf("could not cast peer service of type %T to *kapi.Service", obj)
		}
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerServiceDelete(extraParameters.gp, service)

	case factory.PeerPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorDelete(extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		// when the namespace labels no longer apply
		// remove the namespaces pods from the address_set
		var errs []error
		namespace := obj.(*kapi.Namespace)
		pods, _ := oc.watchFactory.GetPods(namespace.Name)

		for _, pod := range pods {
			if err := oc.handlePeerPodSelectorDelete(extraParameters.gp, pod); err != nil {
				errs = append(errs, err)
			}
		}
		return kerrorsutil.NewAggregate(errs)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorDelete(extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		namespace := obj.(*kapi.Namespace)
		// Remove namespace address set from the *gress policy in cache
		// (done in gress.delNamespaceAddressSet()), and then update ACLs
		return oc.handlePeerNamespaceSelectorOnUpdate(extraParameters.np, extraParameters.gp, func() bool {
			// ... on condition that the removed address set was in the 'gress policy
			return extraParameters.gp.delNamespaceAddressSet(namespace.Name)
		})

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handleLocalPodSelectorDelFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			obj)

	case factory.LocalPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		return oc.handleLocalPodSelectorDelFunc(
			extraParameters.policy,
			extraParameters.np,
			extraParameters.portGroupIngressDenyName,
			extraParameters.portGroupEgressDenyName,
			obj)

	case factory.EgressFirewallType:
		egressFirewall := obj.(*egressfirewall.EgressFirewall)
		if err := oc.deleteEgressFirewall(egressFirewall); err != nil {
			return err
		}
		metrics.UpdateEgressFirewallRuleCount(float64(-len(egressFirewall.Spec.Egress)))
		metrics.DecrementEgressFirewallCount()
		return nil

	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return oc.reconcileEgressIP(eIP, nil)

	case factory.EgressIPNamespaceType:
		namespace := obj.(*kapi.Namespace)
		return oc.reconcileEgressIPNamespace(namespace, nil)

	case factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return oc.reconcileEgressIPPod(pod, nil)

	case factory.EgressNodeType:
		node := obj.(*kapi.Node)
		if err := oc.deleteNodeForEgress(node); err != nil {
			return err
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		if _, hasEgressLabel := nodeLabels[nodeEgressLabel]; hasEgressLabel {
			if err := oc.deleteEgressNode(node.Name); err != nil {
				return err
			}
		}
		return nil

	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return oc.reconcileCloudPrivateIPConfig(cloudPrivateIPConfig, nil)

	default:
		return fmt.Errorf("object type %s not supported", rf.ObjType)
	}
}

// Given a *RetryFramework instance, getSyncResourcesFunc retuns the sync function for a given resource type.
// This will be then called on all existing objects when a watcher is started.
// TODO how can I generalize this? the problem is the access to oc...
// Here oc should NOT appear, while in obj_retry_master it should... boh.
func (oc *Controller) getSyncResourcesFunc(rf *retry.RetryFramework) (func([]interface{}) error, error) {
	klog.Infof("assigning sync func for %s", rf.ObjType)
	var syncFunc func([]interface{}) error

	switch rf.ObjType {
	case factory.PodType:
		syncFunc = oc.syncPodsRetriable

	case factory.PolicyType:
		syncFunc = oc.syncNetworkPolicies

	case factory.NodeType:
		syncFunc = oc.syncNodesRetriable

	case factory.PeerServiceType,
		factory.PeerNamespaceAndPodSelectorType:
		syncFunc = nil

	case factory.PeerPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		syncFunc = func(objs []interface{}) error {
			return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, objs...)
		}

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		syncFunc = func(objs []interface{}) error {
			return oc.handlePeerPodSelectorAddUpdate(extraParameters.gp, objs...)
		}

	case factory.PeerNamespaceSelectorType:
		extraParameters := rf.ExtraParameters.(*NetworkPolicyExtraParameters)
		// the function below will never fail, so there's no point in making it retriable...
		syncFunc = func(i []interface{}) error {
			// This needs to be a write lock because there's no locking around 'gress policies
			extraParameters.np.Lock()
			defer extraParameters.np.Unlock()
			// We load the existing address set into the 'gress policy.
			// Notice that this will make the AddFunc for this initial
			// address set a noop.
			// The ACL must be set explicitly after setting up this handler
			// for the address set to be considered.
			extraParameters.gp.addNamespaceAddressSets(i)
			return nil
		}

	case factory.LocalPodSelectorType:
		syncFunc = rf.SyncFunc // TODO will have to be removed

	case factory.EgressFirewallType:
		syncFunc = oc.syncEgressFirewall

	case factory.EgressIPType:
		syncFunc = oc.syncEgressIPs

	case factory.EgressNodeType:
		syncFunc = oc.initClusterEgressPolicies

	case factory.EgressIPPodType,
		factory.EgressIPNamespaceType,
		factory.CloudPrivateIPConfigType:
		syncFunc = nil

	default:
		return nil, fmt.Errorf("no sync function for object type %s", rf.ObjType)
	}

	return syncFunc, nil
}

// Given an object and its type, isObjectInTerminalState returns true if the object is a in terminal state.
// This is used now for pods that are either in a PodSucceeded or in a PodFailed state.
func isObjectInTerminalState(objType reflect.Type, obj interface{}) bool {
	switch objType {
	case factory.PodType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorType,
		factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return util.PodCompleted(pod)

	default:
		return false
	}
}
