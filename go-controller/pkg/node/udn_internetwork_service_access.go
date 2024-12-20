package node

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	corelisters "k8s.io/client-go/listers/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sigs.k8s.io/knftables"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/controller"
	nodenft "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/nftables"
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

	localCNIPodIPsv4 *nftPodElementsSet
	localCNIPodIPsv6 *nftPodElementsSet

	localHostNetworkedPodCGroups *nftPodElementsSet
}

func NewUDNInterNetworkServiceAccessController(ipv4, ipv6 bool,
	podInformer cache.SharedIndexInformer, // coreinformers.PodInformer, // TODO you can pass the local pod informer actually: traffic will hit nftables on the same node where it got originated
) *UDNInterNetworkServiceAccessController {

	podLister := corev1listers.NewPodLister(podInformer.GetIndexer()) // using LOCAL pod informer
	m := &UDNInterNetworkServiceAccessController{
		podLister:                    podLister,
		ipv4:                         ipv4,
		ipv6:                         ipv6,
		localCNIPodIPsv4:             newNFTPodElementsSet(nftablesDefaultPodIPsWithInterNetworkServiceAccessV4Set, false),
		localCNIPodIPsv6:             newNFTPodElementsSet(nftablesDefaultPodIPsWithInterNetworkServiceAccessV6Set, false),
		localHostNetworkedPodCGroups: newNFTPodElementsSet(nftablesDefaultPodCGroupsWithInterNetworkServiceAccessSet, false),
	}
	controllerConfig := &controller.ControllerConfig[v1.Pod]{
		RateLimiter:    workqueue.NewTypedItemFastSlowRateLimiter[string](time.Second, 5*time.Second, 5),
		Informer:       podInformer, //.Informer(),
		Lister:         podLister.List,
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

	if err = m.setupNFTablesObjects(); err != nil {
		return fmt.Errorf("failed to setup nftables objects for UDN inter-network service access: %w", err)
	}

	// TODO Make sure this is run /after/ the chains are added from gateway_nft...
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
	safeDelete(tx, &knftables.Set{
		Name: nftablesDefaultPodIPsWithInterNetworkServiceAccessV4Set,
		Type: "ipv4_addr",
	})
	safeDelete(tx, &knftables.Set{
		Name: nftablesDefaultPodIPsWithInterNetworkServiceAccessV6Set,
		Type: "ipv6_addr",
	})

	// add set for cgroups
	safeDelete(tx, &knftables.Set{
		Name: nftablesDefaultPodCGroupsWithInterNetworkServiceAccessSet,
		Type: "cgroup",
	})

	// nftablesDefaultPodCGroupsWithInterNetworkServiceAccessSet
	return nft.Run(context.TODO(), tx)
}

// TODO called from Start(), it creates chains and sets
func (m *UDNInterNetworkServiceAccessController) setupNFTablesObjects() error {
	tx := m.nft.NewTransaction()
	// TODO warning: you're alreadying add these in gateway_shared_intf.go
	tx.Add(&knftables.Set{
		Name:    nftablesDefaultPodIPsWithInterNetworkServiceAccessV4Set,
		Comment: knftables.PtrTo("Default to UDN service cluster IP (IPv4)"),
		Type:    "ipv4_addr",
	})
	tx.Add(&knftables.Set{
		Name:    nftablesDefaultPodIPsWithInterNetworkServiceAccessV6Set,
		Comment: knftables.PtrTo("Default to UDN service cluster IP (IPv6)"),
		Type:    "ipv6_addr",
	})

	// m.addRules(tx)

	err := m.nft.Run(context.TODO(), tx)
	if err != nil {
		return fmt.Errorf("could not setup nftables rules for UDN from inter network service access: %v", err)
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

func (m *UDNInterNetworkServiceAccessController) getAllPodsInSelectedNamespaces() ([]*v1.Pod, error) {
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
	cniPodIPsv4 := map[string]sets.Set[string]{}
	cniPodIPsv6 := map[string]sets.Set[string]{}

	hostNetworkedPodCGroups := map[string]sets.Set[string]{}

	pods, err := m.getAllPodsInSelectedNamespaces()

	for _, pod := range pods {
		podKey, err := cache.MetaNamespaceKeyFunc(pod)
		if err != nil {
			klog.Warningf("UDNInterNetworkServiceAccessController failed to get key for pod %s: %v", podKey, err)
			continue
		}
		if util.PodWantsHostNetwork(pod) {
			// TODO
			// retrieve cgroup of the pod
			// cgroup = ...
			// hostNetworkedPodCGroups[podKey] = cgroup
			cgroup, err := getPodCgroupV2(pod)
			if err != nil {
				return err
			}
			hostNetworkedPodCGroups[podKey] = sets.New(cgroup)
		} else {
			// pod is on the CNI network
			// retrieve pod IPs
			pi, err := m.getPodIPs(podKey, pod)
			if err != nil {
				return err
			}
			if pi == nil {
				// this pod doesn't need to be updated
				continue
			}

			cniPodIPsv4[podKey] = pi.ipv4
			cniPodIPsv6[podKey] = pi.ipv6

		}

	}
	if err = m.localCNIPodIPsv4.fullSync(m.nft, cniPodIPsv4); err != nil {
		return err
	}
	if err = m.localCNIPodIPsv6.fullSync(m.nft, cniPodIPsv6); err != nil {
		return err
	}

	if err = m.localHostNetworkedPodCGroups.fullSync(m.nft, hostNetworkedPodCGroups); err != nil {
		return err
	}

	return nil
}

func podNeedsUpdateForInterNetworkServiceAccess(oldObj, newObj *v1.Pod) bool {
	if oldObj == nil || newObj == nil {
		return true
	}
	// react to pod IP changes
	return !reflect.DeepEqual(oldObj.Status.PodIPs, newObj.Status.PodIPs)
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

	isInDefaultNetwork, err := isPodInDefaultClusterNetwork(pod)
	if err != nil {
		return fmt.Errorf("failed to check if pod %s is in default network: %w", key, err)
	}
	if !isInDefaultNetwork {
		return nil
	}

	pi := &podInfoForInterNetwork{}
	if pod.Spec.HostNetwork {
		// TODO
	} else {
		if pi, err = m.getPodIPs(key, pod); err != nil {
			return err
		}
	}
	if pi == nil {
		// this pod doesn't need to be updated
		return nil
	}

	return m.updateWithPodInfo(key, pi)

}

// TODO this type is used to hold multipe information about the same pod in udn_isolation.go
// You actually need hold pod IPs for cni pods and a group for host networked pods.
// This type might be unnecessary.
type podInfoForInterNetwork struct {
	ipv4   sets.Set[string] //ipv4 // TODO QUESTION: why was it a set? There can only be one, no? I think it's because she uses ports, but for default pod ips  there can only be one per ip family
	ipv6   sets.Set[string] //string
	cgroup sets.Set[string] //string
}

// getPodIPs returns nftables set elements for an ovnk-networked pod in the default network.
// nil is returned when pod should not be updated.
// empty podInfoForInterNetwork will delete the pod from all sets and is returned when nil pod is passed.
// first error is for parsing openPorts annotation, second error is for fetching pod IPs.
// parsing error should not stop the update, as we need to cleanup potentially present rules from the previous config.
func (m *UDNInterNetworkServiceAccessController) getPodIPs(podKey string, pod *v1.Pod) (*podInfoForInterNetwork, error) {
	// TODO maybe get IPs + cgroup here? if not hostnetworked get ips, otherwise get cgroup?
	pi := &podInfoForInterNetwork{}
	if pod == nil {
		return pi, nil
	}

	podIPs, err := util.DefaultNetworkPodIPs(pod)
	if err != nil {
		// update event should come later with ips
		klog.V(5).Infof("Failed to get default network pod IPs for pod %s: %v", podKey, err)
		return nil, nil
	}
	pi.ipv4, pi.ipv6 = splitIPsPerFamily(podIPs)
	return pi, nil
}

func getPodCgroupV2(pod *v1.Pod) (string, error) {
	// Get any container ID
	if len(pod.Status.ContainerStatuses) == 0 {
		return "", fmt.Errorf("no containers in pod")
	}

	containerID := pod.Status.ContainerStatuses[0].ContainerID

	containerPID, err := getContainerPID(containerID)
	if err != nil {
		return "", err
	}

	cgroupFile := fmt.Sprintf("/proc/%d/cgroup", containerPID)
	content, err := os.ReadFile(cgroupFile)
	if err != nil {
		return "", err
	}

	// Parse the cgroup file
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	for scanner.Scan() {
		line := scanner.Text()
		// For cgroup v2, we'll see a single line with "0::/path"
		if strings.HasPrefix(line, "0::") {
			return strings.TrimPrefix(line, "0::"), nil
		}
	}

	return "", fmt.Errorf("cgroup not found")
}

// TODO
// How do you implement this in go?
func getContainerPID(containerID string) (int, error) {
	// This implementation depends on your container runtime
	// You might need to use the runtime's API to get the PID
	// For example, with containerd:
	// Use containerd client to get container PID
	// crictl inspect $CONTAINER_ID | jq .info.pid
	return 0, fmt.Errorf("not implemented")
}

// updateWithPodInfo updates the nftables sets with given podInfoForInterNetwork for a given pod.
// empty podInfoForInterNetwork will delete the pod from all sets.
// TODO update also for localhost
// TODO problem: upon delete, how can you tell whether it was a cni pod or a host networked pod?
// maybe delete in all sets?
func (m *UDNInterNetworkServiceAccessController) updateWithPodInfo(podKey string, pi *podInfoForInterNetwork) error {
	tx := m.nft.NewTransaction()
	m.localCNIPodIPsv4.updatePodElementsTX(podKey, pi.ipv4, tx)
	m.localCNIPodIPsv6.updatePodElementsTX(podKey, pi.ipv6, tx)

	if tx.NumOperations() == 0 {
		return nil
	}

	err := m.nft.Run(context.TODO(), tx)
	if err != nil {
		return fmt.Errorf("could not update nftables set for UDN pods: %v", err)
	}

	// update internal state only after successful transaction
	m.localCNIPodIPsv4.updatePodElementsAfterTX(podKey, pi.ipv4)
	m.localCNIPodIPsv6.updatePodElementsAfterTX(podKey, pi.ipv6)
	return nil
}
