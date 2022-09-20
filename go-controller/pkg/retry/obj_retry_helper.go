package retry

import (
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

// helper functions to lock RetryFramework properly and inspect retry entry fields in unit tests

const (
	inspectTimeout = 4 * time.Second // arbitrary, to avoid failures on github CI
)

func CheckRetryObj(key string, rf *RetryFramework) bool {
	rf.retryEntries.LockKey(key)
	defer rf.retryEntries.UnlockKey(key)
	_, found := rf.getRetryObj(key)
	return found
}

func GetRetryObj(key string, rf *RetryFramework) (*retryObjEntry, bool) {
	rf.retryEntries.LockKey(key)
	defer rf.retryEntries.UnlockKey(key)
	return rf.getRetryObj(key)
}

func GetOldObjFromRetryObj(key string, rf *RetryFramework) interface{} {
	rf.retryEntries.LockKey(key)
	defer rf.retryEntries.UnlockKey(key)
	obj, exists := rf.getRetryObj(key)
	if exists && obj != nil {
		return obj.oldObj
	}
	return nil
}

func GetNewObjFromRetryObj(key string, rf *RetryFramework) interface{} {
	rf.retryEntries.LockKey(key)
	defer rf.retryEntries.UnlockKey(key)
	obj, exists := rf.getRetryObj(key)
	if exists && obj != nil {
		return obj.newObj
	}
	return nil
}

func GetConfigFromRetryObj(key string, rf *RetryFramework) interface{} {
	rf.retryEntries.LockKey(key)
	defer rf.retryEntries.UnlockKey(key)
	obj, exists := rf.getRetryObj(key)
	if exists && obj != nil {
		return obj.config
	}
	return nil
}

func SetFailedAttemptsCounterForTestingOnly(key string, val uint8, rf *RetryFramework) {
	rf.DoWithLock(key, func(key string) {
		entry, found := rf.getRetryObj(key)
		if found {
			entry.failedAttempts = val
		}
	})
}

func SetRetryObjWithNoBackoff(key string, rf *RetryFramework) {
	rf.DoWithLock(key, func(key string) {
		entry, found := rf.getRetryObj(key)
		if found {
			rf.setRetryObjWithNoBackoff(entry)
		}
	})
}

func InitRetryObjWithAdd(obj interface{}, key string, rf *RetryFramework) {
	rf.DoWithLock(key, func(key string) {
		rf.initRetryObjWithAdd(obj, key)
	})
}

func DeleteRetryObj(key string, rf *RetryFramework) {
	rf.DoWithLock(key, func(key string) {
		rf.DeleteRetryObj(key)
	})
}

func CheckRetryObjectEventually(key string, shouldExist bool, rf *RetryFramework) {
	expectedValue := gomega.BeTrue()
	if !shouldExist {
		expectedValue = gomega.BeFalse()
	}
	gomega.Eventually(func() bool {
		return CheckRetryObj(key, rf)
	}).Should(expectedValue)
}

// func CheckRetryObjectFailedAttemptsEventually(key string, expectedValue uint8, rf *RetryFramework) {
// 	expected := gomega.BeNumerically("==", expectedValue)
// 	gomega.Eventually(func() uint8 {
// 		obj, exists := GetRetryObj(key, rf)
// 		if exists {
// 			return obj.failedAttempts //, nil
// 		}
// 		return 0 //, fmt.Errorf("object not found")
// 	}).Should(expected)
// }

// func CheckRetryObjectNewObjectEventually(key string, expectedMatch types.GomegaMatcher, rf *RetryFramework) {
// 	// expectedValue := gomega.Not(gomega.BeNil())
// 	// if !shouldExist {
// 	// 	expectedValue = gomega.BeNil()
// 	// }
// 	gomega.Eventually(func() interface{} {
// 		obj, exists := GetRetryObj(key, rf)
// 		if exists {
// 			return obj.newObj //, nil
// 		}
// 		return nil //, fmt.Errorf("object not found")
// 	}).Should(expectedMatch)
// }

// func CheckRetryObjectOldObjectEventually(key string, expectedMatch types.GomegaMatcher, rf *RetryFramework) {
// 	// expectedValue := gomega.Not(gomega.BeNil())
// 	// if !shouldExist {
// 	// 	expectedValue = gomega.BeNil()
// 	// }
// 	gomega.Eventually(func() interface{} {
// 		obj, exists := GetRetryObj(key, rf)
// 		if exists {
// 			return obj.oldObj //, nil
// 		}
// 		return nil //, fmt.Errorf("object not found")
// 	}).Should(expectedMatch)
// }

// func CheckRetryObjectConfigEventually(key string, expectedMatch types.GomegaMatcher, rf *RetryFramework) {
// 	// expectedValue := gomega.Not(gomega.BeNil())
// 	// if !shouldExist {
// 	// 	expectedValue = gomega.BeNil()
// 	// }
// 	gomega.Eventually(func() interface{} {
// 		obj, exists := GetRetryObj(key, rf)
// 		if exists {
// 			return obj.config //, nil
// 		}
// 		return nil //, fmt.Errorf("object not found")
// 	}).Should(expectedMatch)
// }

func CheckRetryObjectMultipleFieldsEventually(
	key string,
	rf *RetryFramework,
	expectedParams ...types.GomegaMatcher) {
	// expectedNewObj types.GomegaMatcher,
	// expectedConfig types.GomegaMatcher,
	// expectedFailedAttempts types.GomegaMatcher,
	// rf *RetryFramework) {
	// expectedValue := gomega.Not(gomega.BeNil())
	// if !shouldExist {
	// 	expectedValue = gomega.BeNil()
	// }
	var expectedNewObj, expectedOldObj, expectedConfig, expectedFailedAttempts types.GomegaMatcher
	for i, expectedParam := range expectedParams {
		if i == 0 {
			expectedOldObj = expectedParam
		} else if i == 1 {
			expectedNewObj = expectedParam
		} else if i == 2 {
			expectedConfig = expectedParam
		} else if i == 3 {
			expectedFailedAttempts = expectedParam
		}
	}
	gomega.Eventually(func(g gomega.Gomega) {
		obj, exists := GetRetryObj(key, rf)
		g.Expect(exists).To(gomega.BeTrue())
		g.Expect(obj).NotTo(gomega.BeNil())
		if exists {
			if expectedOldObj != nil {
				g.Expect(obj.oldObj).To(expectedOldObj)
			}
			if expectedNewObj != nil {
				g.Expect(obj.newObj).To(expectedNewObj)
			}
			if expectedConfig != nil {
				g.Expect(obj.config).To(expectedConfig)
			}
			if expectedFailedAttempts != nil {
				g.Expect(obj.failedAttempts).To(expectedFailedAttempts)
			}

		}
	}, inspectTimeout).Should(gomega.Succeed())
}

func RetryObjsLen(rf *RetryFramework) int {
	return len(rf.retryEntries.GetKeys())
}
