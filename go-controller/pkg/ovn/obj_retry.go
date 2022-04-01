package ovn

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"k8s.io/klog/v2"

	factory "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
)

type retryObjEntry struct {
	newObj     interface{}
	oldObj     interface{}
	config     interface{}
	timeStamp  time.Time
	backoffSec time.Duration
	// whether to include this object in the retry iterations
	ignore bool
}

// initRetryObj tracks an object that failed to be created to potentially retry later
// initially it is marked as skipped for retry loop (ignore = true)
func (r *retryObjs) initRetryObj(obj interface{}, key string) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.timeStamp = time.Now()
		entry.newObj = obj
	} else {
		r.entries[key] = &retryObjEntry{newObj: obj, oldObj: nil, config: nil,
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
		r.entries[key] = &retryObjEntry{newObj: nil, oldObj: obj, config: config,
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

// checkAndDeleteRetryObj deletes a specific entry from the map, if it existed, returns true
func (r *retryObjs) checkAndDeleteRetryObj(key string, withLock bool) bool {
	if withLock {
		r.retryMutex.Lock()
		defer r.retryMutex.Unlock()
	}
	if _, ok := r.entries[key]; ok {
		delete(r.entries, key)
		return true
	}
	return false
}

// checkAndSkipRetryObj sets a specific entry from the map to be ignored for subsequent retries
// if it existed, returns true
func (r *retryObjs) checkAndSkipRetryObj(key string) bool {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		entry.ignore = true
		return true
	}
	return false
}

// requestRetryObjs allows a caller to immediately request to iterate through all objects that
// are in the retry cache. This will ignore any outstanding time wait/backoff state
// Used for Unit Tests only
func (r *retryObjs) requestRetryObjs() {
	select {
	case r.retryChan <- struct{}{}:
		klog.V(5).Infof("Iterate retry object requested")
	default:
		klog.V(5).Infof("Iterate retry object already requested")
	}
}

// getObjRetryEntry returns a copy of an object  retry entry from the cache
// Used for Unit Tests only
func (r *retryObjs) getObjRetryEntry(key string) *retryObjEntry {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()
	if entry, ok := r.entries[key]; ok {
		x := *entry
		return &x
	}
	return nil
}

func splitNamespacedName(namespacedName string) (string, string) {
	sep := "/"
	if strings.Contains(namespacedName, sep) {
		s := strings.SplitN(namespacedName, sep, 1)
		if len(s) == 2 {
			return s[0], s[1]
		}
	}
	return namespacedName, ""

}

func HasResourceAnUpdateFunc(objType reflect.Type) (bool, error) {
	// switch based on type
	switch objType {
	case factory.PolicyType:
		// var kNP *knet.NetworkPolicy
		return false, nil
	case factory.NodeType:
		return true, nil
	default:
		return false, fmt.Errorf("object type %v not supported", objType) // default error message
	}
}

func areResourceObjectsEqual(objType reflect.Type, obj1, obj2 interface{}) (bool, error) {
	var err error
	// switch based on type
	switch objType {
	case factory.PolicyType:
		// var kNP *knet.NetworkPolicy
		kNP1, ok := obj1.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast interface{} object to *knet.NetworkPolicy")
		}
		kNP2, ok := obj2.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast interface{} object to *knet.NetworkPolicy")
		}
		return reflect.DeepEqual(kNP1, kNP2), nil
	case factory.NodeType:
		kNode1, ok := obj1.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast interface{} object to *kapi.Node")
		}
		kNode2, ok := obj2.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast interface{} object to *kapi.Node")
		}
		return reflect.DeepEqual(kNode1, kNode2), nil
	default:
		err = fmt.Errorf("object type %v not supported", objType) // default error message
	}
	return false, err
}

func GetResourceObjectKey(objType reflect.Type, obj interface{}) (string, error) {
	var err error
	// switch based on type
	switch objType {
	case factory.PolicyType:
		// var kNP *knet.NetworkPolicy
		kNP, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return "", fmt.Errorf("could not cast interface{} object to *knet.NetworkPolicy")
		}
		key := getPolicyNamespacedName(kNP)
		return key, nil
	case factory.NodeType:
		kNode, ok := obj.(*kapi.Node)
		if !ok {
			return "", fmt.Errorf("could not cast interface{} object to *kapi.Node")
		}
		return kNode.Name, nil
	default:
		err = fmt.Errorf("object type %v not supported", objType) // default error message
	}
	return "", err
}

func (oc *Controller) GetResourceObjectFromInformerCache(objType reflect.Type, key string) (interface{}, error) {
	var obj interface{}
	var err error
	// switch based on type
	switch objType {
	case factory.PolicyType:
		// var ok bool
		// var kNP *knet.NetworkPolicy
		namespace, name := splitNamespacedName(key)
		obj, err = oc.watchFactory.GetNetworkPolicy(namespace, name)
		return obj, err
	case factory.NodeType:
		obj, err = oc.watchFactory.GetNode(key)
	default:
		err = fmt.Errorf("object type %v not supported", objType) // default error message
	}
	return obj, err
}

func (oc *Controller) addResourceObject(objType reflect.Type, obj interface{}) error {
	// switch based on type
	var err error
	switch objType {
	case factory.PolicyType:
		kNP, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast interface{} object to *knet.NetworkPolicy")
		}
		klog.Infof("addResourceObject called on network policy %s/%s",
			kNP.Namespace, kNP.Name)

		if err = oc.addNetworkPolicy(kNP); err != nil {
			klog.Infof("Network Policy retry delete failed for %s/%s, will try again later: %v",
				kNP.Namespace, kNP.Name, err)
			return err
		}
	case factory.NodeType:
		kNode, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast interface{} object to *kapi.Node")
		}
		if err = oc.addNodeObj(kNode); err != nil {
			klog.Infof("Node retry delete failed for %s, will try again later: %v",
				kNode.Name, err)
			return err
		}
	default:
		return fmt.Errorf("object type %v not supported", objType) // default error message
	}

	return nil
}

func (oc *Controller) updateResourceObject(objType reflect.Type, newObj, oldObj interface{}) error {
	// switch based on type
	if hasUpdateFunc, err := HasResourceAnUpdateFunc(objType); !hasUpdateFunc || err != nil {
		return fmt.Errorf("no update function for resource type %v: %v", objType, err)
	}

	switch objType {
	case factory.PolicyType:
		return fmt.Errorf("no update function for network policies")
	case factory.NodeType:
		newNode, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast newObj of type interface{} to *kapi.Node")
		}
		oldNode, ok := oldObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast oldObj of type interface{} to *kapi.Node")
		}

		return oc.updateNodeObj(newNode, oldNode)

	default:
		return fmt.Errorf("object type %v not supported", objType) // default error message
	}

	return nil
}

// func watchFactory.GetNetworkPolicy(objMeta.Namespace, objMeta.Name)
// oc.watchFactory.GetPod(pod.Namespace, pod.Name)
func (oc *Controller) deleteResourceObject(objType reflect.Type, kObj, cachedObj interface{}) error {
	klog.Infof("deleteResourceObject called on objectType %v", objType)
	// switch based on type
	switch objType {
	case factory.PolicyType:
		var cachedNP *networkPolicy
		kNP, ok := kObj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast interface{} object to *knet.NetworkPolicy")
		}
		if cachedObj != nil {
			if cachedNP, ok = cachedObj.(*networkPolicy); !ok {
				cachedNP = nil
			}
		}
		klog.Infof("deleteResourceObject called on network policy %s/%s", kNP.Namespace, kNP.Name)

		if err := oc.deleteNetworkPolicy(kNP, cachedNP); err != nil {
			klog.Infof("Network Policy retry delete "+
				"failed for %s/%s, will try again later: %v",
				kNP.Namespace, kNP.Name, err)
			return err

		}
	case factory.NodeType:
		kNode, ok := kObj.(*kapi.Node)
		klog.Infof("deleteResourceObject called on node %s", kNode.Name)

		if !ok {
			return fmt.Errorf("could not cast interface{} object to *knet.Node")
		}
		if err := oc.deleteNodeObj(kNode); err != nil {
			klog.Infof("Node retry delete failed for %s, will try again later: %v",
				kNode.Name, err)
			return err
		}
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
		// check if we need to create
		var objectToCreate interface{}
		if entry.newObj != nil {
			// get the latest version of the resource object from the informer;
			// if it doesn't exist we are not going to create the new object.
			// objMeta, err := factory.GetObjectMeta(r.oType, entry.newObj) // not in use now
			// np, err := oc.watchFactory.GetNetworkPolicy(objMeta.Namespace, objMeta.Name)
			obj, err := oc.GetResourceObjectFromInformerCache(r.oType, objKey)
			if err != nil && kerrors.IsNotFound(err) {
				klog.Infof("%s %s not found in the informers cache,"+
					" not going to retry object create", r.oTypeName, objKey)
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
		if updateAll || now.After(objTimer) {
			klog.Infof("%s retry: %s retry object setup",
				r.oTypeName, objKey)

			// check if we need to delete anything
			if entry.oldObj != nil {
				klog.Infof("%s retry: removing old object for %s",
					r.oTypeName, objKey)
				if err := oc.deleteResourceObject(r.oType, entry.oldObj, entry.config); err != nil {
					klog.Infof("%s retry delete failed for %s, will try again later: %v",
						r.oTypeName, objKey, err)
					entry.timeStamp = time.Now()
					continue
				}
				// successfully cleaned up old object, remove it from the retry cache
				entry.oldObj = nil
			}

			// create new object if needed
			if objectToCreate != nil {
				klog.Infof("%s retry: Creating object for %s",
					r.oTypeName, objKey)
				if err := oc.addResourceObject(r.oType, objectToCreate); err != nil {
					klog.Infof("%s retry create "+
						"failed for %s, will try again later: %v",
						r.oTypeName, objKey, err)
					entry.timeStamp = time.Now()
					continue
				}
				// successfully cleaned up old object, remove it from the retry cache
				entry.newObj = nil
			}

			klog.Infof("%s retry successful for %s", r.oTypeName, objKey)
			oc.retryNetPolices.checkAndDeleteRetryObj(objKey, false)
		} else {
			klog.V(5).Infof("%s retry %s not after timer yet, time: %s",
				r.oTypeName, objKey, objTimer)
		}
	}
}

func (oc *Controller) periodicallyRetryResourceObjects(r *retryObjs) {
	// track the retryObjs map and every 30 seconds check if any object needs to be retried
	for {
		select {
		case <-time.After(retryObjInterval):
			oc.iterateRetryResourceObjects(r, false)

		case <-oc.retryNodes.retryChan:
			oc.iterateRetryResourceObjects(r, true)

		case <-oc.stopChan:
			return
		}
	}
}

func (oc *Controller) getSyncResourceObjectsFunc(objType reflect.Type) (func([]interface{}), error) {
	var syncRetriableFunc func([]interface{}) error
	var syncFunc func([]interface{})

	var name string
	// switch based on type
	switch objType {
	case factory.PolicyType:
		name = "syncNetworkPolicies"
		syncRetriableFunc = oc.syncNetworkPoliciesRetriable
	case factory.NodeType:
		name = "syncNodes"
		syncRetriableFunc = oc.syncNodesRetriable
	default:
		return nil, fmt.Errorf("object type %v not supported", objType) // default error message
	}

	syncFunc = func(objects []interface{}) {
		klog.Infof("I am a sync func!")
		oc.syncWithRetry(name, func() error { return syncRetriableFunc(objects) })
	}
	return syncFunc, nil

}
