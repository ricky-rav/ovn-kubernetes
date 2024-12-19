package node

import (
	"context"
	"fmt"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sigs.k8s.io/knftables"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/controller"
	nad "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/network-attach-def-controller"
	nodenft "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/nftables"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// UDNHostIsolationManager manages the host isolation for user defined networks.
// It uses nftables chain "udn-isolation" to only allow connection to primary UDN pods from kubelet.
// It also listens to systemd events to re-apply the rules after kubelet restart as cgroup matching is used.
type UDNInterNetworkServiceAccessController struct {
	nft           knftables.Interface
	ipv4, ipv6    bool
	podController controller.Controller
	podLister     corelisters.PodLister
	nadController *nad.NetAttachDefinitionController

	udnPodIPsv4 *nftPodElementsSetForInterNetwork
	udnPodIPsv6 *nftPodElementsSetForInterNetwork
}

func NewUDNInterNetworkServiceAccessController(
	podInformer coreinformers.PodInformer,
	nadController *nad.NetAttachDefinitionController) *UDNInterNetworkServiceAccessController {

	m := &UDNInterNetworkServiceAccessController{
		podLister:     podInformer.Lister(),
		nadController: nadController,
	}
	controllerConfig := &controller.ControllerConfig[v1.Pod]{
		RateLimiter:    workqueue.NewTypedItemFastSlowRateLimiter[string](time.Second, 5*time.Second, 5),
		Informer:       podInformer.Informer(),
		Lister:         podInformer.Lister().List,
		ObjNeedsUpdate: podNeedsUpdateForInterNetworkServiceAccess,
		Reconcile:      m.reconcilePod,
		Threadiness:    1,
	}
	m.podController = controller.NewController[v1.Pod]("udn-inter-network-service-access-controller", controllerConfig)
	return m
}

// Start must be called on node setup.
func (m *UDNInterNetworkServiceAccessController) Start(ctx context.Context) error {
	nft, err := nodenft.GetNFTablesHelper()
	if err != nil {
		return fmt.Errorf("failed getting nftables helper: %w", err)
	}

	m.nft = nft

	// TODO create vmap if it doesn't exist
	// TODO ONly start this once we have the vmap and chain1 (chain-to-udn)
	return controller.StartWithInitialSync(m.podInitialSync, m.podController)
}

func (m *UDNInterNetworkServiceAccessController) Stop() {
	controller.Stop(m.podController)
}

// CleanupUDNInterNetworkServiceAccess removes all nftables chains and sets created by UDNInterNetworkServiceAccessController.
func CleanupUDNInterNetworkServiceAccess() error {
	nft, err := nodenft.GetNFTablesHelper()
	if err != nil {
		return fmt.Errorf("failed getting nftables helper: %w", err)
	}
	tx := nft.NewTransaction()
	safeDelete(tx, &knftables.Chain{
		Name: UDNIsolationChain,
	})
	safeDelete(tx, &knftables.Set{
		Name: nftablesUDNPodIPsv4,
		Type: "ipv4_addr",
	})
	safeDelete(tx, &knftables.Set{
		Name: nftablesUDNPodIPsv6,
		Type: "ipv6_addr",
	})
	return nft.Run(context.TODO(), tx)
}

func (m *UDNInterNetworkServiceAccessController) setupUDNIsolationFromHost() error {
	tx := m.nft.NewTransaction()
	tx.Add(&knftables.Chain{
		Name:     UDNIsolationChain,
		Comment:  knftables.PtrTo("Host isolation for user defined networks"),
		Type:     knftables.PtrTo(knftables.FilterType),
		Hook:     knftables.PtrTo(knftables.OutputHook),
		Priority: knftables.PtrTo(knftables.FilterPriority),
	})
	tx.Flush(&knftables.Chain{
		Name: UDNIsolationChain,
	})
	tx.Add(&knftables.Set{
		Name:    nftablesUDNPodIPsv4,
		Comment: knftables.PtrTo("default network IPs of pods in user defined networks (IPv4)"),
		Type:    "ipv4_addr",
	})
	tx.Add(&knftables.Set{
		Name:    nftablesUDNPodIPsv6,
		Comment: knftables.PtrTo("default network IPs of pods in user defined networks (IPv6)"),
		Type:    "ipv6_addr",
	})
	m.addRules(tx)

	err := m.nft.Run(context.TODO(), tx)
	if err != nil {
		return fmt.Errorf("could not setup nftables rules for UDN from host isolation: %v", err)
	}
	return nil
}

func (m *UDNInterNetworkServiceAccessController) addRules(tx *knftables.Transaction) {
	if m.ipv4 {

		tx.Add(&knftables.Rule{
			Chain: UDNIsolationChain,
			Rule: knftables.Concat(
				"ip", "daddr", "@", nftablesUDNPodIPsv4, "drop"),
		})
	}
	if m.ipv6 {
		tx.Add(&knftables.Rule{
			Chain: UDNIsolationChain,
			Rule: knftables.Concat(
				"ip6", "daddr", "@", nftablesUDNPodIPsv6, "drop"),
		})
	}
}

// TODO for individual pod lookup, just use util.HasNamespaceAccessToUDNServices

func (m *UDNInterNetworkServiceAccessController) getPodsInSelectedNamespaces() ([]*v1.Pod, error) {
	var pods []*v1.Pod
	for _, namespace := range config.Default.NamespacesForInterNetworkServiceAccess {
		podsInNamespace, err := m.podLister.Pods(namespace).List(labels.Everything())
		if err != nil {
			return nil, fmt.Errorf("failed to list pods in selected namespace %s: %v", namespace, err)
		}
		pods = append(pods, podsInNamespace...)
	}
	return pods, nil
}

func (m *UDNInterNetworkServiceAccessController) podInitialSync() error {
	udnPodIPsv4 := map[string]sets.Set[string]{}
	udnPodIPsv6 := map[string]sets.Set[string]{}

	pods, err := m.getPodsInSelectedNamespaces()

	for _, pod := range pods {
		podKey, err := cache.MetaNamespaceKeyFunc(pod)
		if err != nil {
			klog.Warningf("UDNInterNetworkServiceAccessController failed to get key for pod %s in namespace %s: %v", pod.Name, pod.Namespace, err)
			continue
		}
		// ignore openPorts parse error in initial sync
		pi, err := m.getPodInfo(podKey, pod)
		if err != nil {
			return err
		}
		if pi == nil {
			// this pod doesn't need to be updated
			continue
		}

		udnPodIPsv4[podKey] = pi.ipsv4
		udnPodIPsv6[podKey] = pi.ipsv6

	}
	if err = m.udnPodIPsv4.fullSync(m.nft, udnPodIPsv4); err != nil {
		return err
	}
	if err = m.udnPodIPsv6.fullSync(m.nft, udnPodIPsv6); err != nil {
		return err
	}

	return nil
}

func podNeedsUpdateForInterNetworkServiceAccess(oldObj, newObj *v1.Pod) bool {
	if oldObj == nil || newObj == nil {
		return true
	}
	// react to pod IP changes
	return !reflect.DeepEqual(oldObj.Status, newObj.Status) ||
		oldObj.Annotations[util.OvnPodAnnotationName] != newObj.Annotations[util.OvnPodAnnotationName]
}

func (m *UDNInterNetworkServiceAccessController) reconcilePod(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Errorf("UDNInterNetworkServiceAccessController failed to split meta namespace cache key %s for pod: %v", key, err)
		return nil
	}
	pod, err := m.podLister.Pods(namespace).Get(name)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// Pod was deleted, clean up.
			return m.updateWithPodInfo(key, &podInfoForInterNetwork{})
		}
		return fmt.Errorf("failed to fetch pod %s in namespace %s", name, namespace)
	}
	pi, err := m.getPodInfo(key, pod)
	if err != nil {
		return err
	}
	if pi == nil {
		// this pod doesn't need to be updated
		return nil
	}
	return m.updateWithPodInfo(key, pi)

}

type podInfoForInterNetwork struct {
	ipsv4       sets.Set[string]
	ipsv6       sets.Set[string]
	icmpv4      sets.Set[string]
	icmpv6      sets.Set[string]
	openPortsv4 sets.Set[string]
	openPortsv6 sets.Set[string]
}

// getPodInfo returns nftables set elements for a pod.
// nil is returned when pod should not be updated.
// empty podInfoForInterNetwork will delete the pod from all sets and is returned when nil pod is passed.
// first error is for parsing openPorts annotation, second error is for fetching pod IPs.
// parsing error should not stop the update, as we need to cleanup potentially present rules from the previous config.
func (m *UDNInterNetworkServiceAccessController) getPodInfo(podKey string, pod *v1.Pod) (*podInfoForInterNetwork, error) {
	pi := &podInfoForInterNetwork{}
	if pod == nil {
		return pi, nil
	}
	// only add pods with primary UDN
	primaryUDN, err := m.isPodPrimaryUDN(pod)
	if err != nil {
		return nil, fmt.Errorf("failed to check if pod %s is in primary UDN: %w", podKey, err)
	}
	if !primaryUDN {
		return nil, nil
	}
	podIPs, err := util.DefaultNetworkPodIPs(pod)
	if err != nil {
		// update event should come later with ips
		klog.V(5).Infof("Failed to get default network pod IPs for pod %s: %v", podKey, err)
		return nil, nil
	}
	pi.ipsv4, pi.ipsv6 = splitIPsPerFamily(podIPs)
	return pi, nil
}

// updateWithPodInfo updates the nftables sets with given podInfoForInterNetwork for a given pod.
// empty podInfoForInterNetwork will delete the pod from all sets.
func (m *UDNInterNetworkServiceAccessController) updateWithPodInfo(podKey string, pi *podInfoForInterNetwork) error {
	tx := m.nft.NewTransaction()
	m.udnPodIPsv4.updatePodElementsTX(podKey, pi.ipsv4, tx)
	m.udnPodIPsv6.updatePodElementsTX(podKey, pi.ipsv6, tx)

	if tx.NumOperations() == 0 {
		return nil
	}

	err := m.nft.Run(context.TODO(), tx)
	if err != nil {
		return fmt.Errorf("could not update nftables set for UDN pods: %v", err)
	}

	// update internal state only after successful transaction
	m.udnPodIPsv4.updatePodElementsAfterTX(podKey, pi.ipsv4)
	m.udnPodIPsv6.updatePodElementsAfterTX(podKey, pi.ipsv6)
	return nil
}

func (m *UDNInterNetworkServiceAccessController) isPodPrimaryUDN(pod *v1.Pod) (bool, error) {
	podAnnotation, err := util.UnmarshalPodAnnotation(pod.Annotations, types.DefaultNetworkName)
	if err != nil {
		// pod IPs were not assigned yet, should be retried later
		return false, err
	}
	// NetworkRoleInfrastructure means default network is not primary, then UDN must be the primary network
	return podAnnotation.Role == types.NetworkRoleInfrastructure, nil
}

func (m *UDNInterNetworkServiceAccessController) getOpenPortSets(newV4IPs, newV6IPs sets.Set[string], openPorts []*util.OpenPort) (icmpv4, icmpv6, openPortsv4, openPortsv6 sets.Set[string]) {
	icmpv4 = sets.New[string]()
	icmpv6 = sets.New[string]()
	openPortsv4 = sets.New[string]()
	openPortsv6 = sets.New[string]()

	for _, openPort := range openPorts {
		if openPort.Protocol == "icmp" {
			icmpv4 = newV4IPs
			icmpv6 = newV6IPs
		} else {
			for podIPv4 := range newV4IPs {
				openPortsv4.Insert(joinNFTSlice([]string{podIPv4, openPort.Protocol, fmt.Sprintf("%d", *openPort.Port)}))
			}
			for podIPv6 := range newV6IPs {
				openPortsv6.Insert(joinNFTSlice([]string{podIPv6, openPort.Protocol, fmt.Sprintf("%d", *openPort.Port)}))
			}
		}
	}
	return
}

// nftPodElementsSetForInterNetwork is a helper struct to manage an nftables set with pod-owned elements.
// Can be used to store pod IPs, or more complex elements.
type nftPodElementsSetForInterNetwork struct {
	setName string
	// podName: set elements
	podElements map[string]sets.Set[string]
	// podIPs may be reused as soon as the pod reaches Terminating state, and delete event may come later.
	// That means a new pod with the same IP may be added before the previous pod is deleted.
	// To avoid deleting newly-added pod IP thinking we are deleting old pod IP, we keep track of re-used set elements.
	elementToPods map[string]sets.Set[string]
	// if a set element is composed of multiple strings
	// set to false to avoid unneeded parsing
	composedValue bool
}

func newNFTPodElementsSetForInterNetwork(setName string, composedValue bool) *nftPodElementsSetForInterNetwork {
	return &nftPodElementsSetForInterNetwork{
		setName:       setName,
		composedValue: composedValue,
		podElements:   make(map[string]sets.Set[string]),
		elementToPods: make(map[string]sets.Set[string]),
	}
}

func (n *nftPodElementsSetForInterNetwork) getKey(key string) []string {
	if n.composedValue {
		return splitNFTSlice(key)
	}
	return []string{key}
}

// updatePodElementsTX adds transaction operations to update pod elements in nftables set.
// To update internal struct, updatePodElementsAfterTX must be called if transaction is successful.
func (n *nftPodElementsSetForInterNetwork) updatePodElementsTX(namespacedName string, podElements sets.Set[string], tx *knftables.Transaction) {
	if n.podElements[namespacedName].Equal(podElements) {
		return
	}
	// always delete all old elements, then add new elements.
	for existingElem := range n.podElements[namespacedName] {
		if n.elementToPods[existingElem].Len() == 1 {
			// only delete element is it referenced by one pod
			tx.Delete(&knftables.Element{
				Set: n.setName,
				Key: n.getKey(existingElem),
			})
		}
	}
	for newElem := range podElements {
		// adding existing element is a no-op
		tx.Add(&knftables.Element{
			Set: n.setName,
			Key: n.getKey(newElem),
		})
	}
}

func (n *nftPodElementsSetForInterNetwork) updatePodElementsAfterTX(namespacedName string, elements sets.Set[string]) {
	for existingElem := range n.podElements[namespacedName] {
		if !elements.Has(existingElem) {
			// element was removed
			n.elementToPods[existingElem].Delete(namespacedName)
			if n.elementToPods[existingElem].Len() == 0 {
				delete(n.elementToPods, existingElem)
			}
		}
	}

	for elem := range elements {
		if n.elementToPods[elem] == nil {
			n.elementToPods[elem] = sets.New[string]()
		}
		n.elementToPods[elem].Insert(namespacedName)
	}
	if len(elements) == 0 {
		delete(n.podElements, namespacedName)
	} else {
		n.podElements[namespacedName] = elements
	}
}

// fullSync should be called on restart to sync all pods elements.
// It flushes existing elements, and adds new elements.
func (n *nftPodElementsSetForInterNetwork) fullSync(nft knftables.Interface, podsElements map[string]sets.Set[string]) error {
	tx := nft.NewTransaction()
	tx.Flush(&knftables.Set{
		Name: n.setName,
	})
	for podName, podElements := range podsElements {
		if len(podElements) == 0 {
			continue
		}
		for elem := range podElements {
			tx.Add(&knftables.Element{
				Set: n.setName,
				Key: n.getKey(elem),
			})
			if n.elementToPods[elem] == nil {
				n.elementToPods[elem] = sets.New[string]()
			}
			n.elementToPods[elem].Insert(podName)
		}
		n.podElements[podName] = podElements
	}
	err := nft.Run(context.TODO(), tx)
	if err != nil {
		clear(n.podElements)
		return fmt.Errorf("initial pods sync for inter network service access failed: %w", err)
	}
	return nil
}
