package ovn

import (
	"time"

	"k8s.io/klog/v2"
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
