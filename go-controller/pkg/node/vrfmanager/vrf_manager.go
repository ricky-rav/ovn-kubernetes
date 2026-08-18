// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package vrfmanager

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/node/routemanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	utilerrors "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util/errors"
)

// reconcile period for vrf manager, this would kick in for every 60 seconds if there is no
// explicit link update events. In the event of link update, reconcile period is automatically
// extended by another 60 seconds.
var reconcilePeriod = 60 * time.Second

type vrf struct {
	name  string
	table uint32
	// managedSlaves are the desired netlink interfaces whose master will be
	// this VRF.
	managedSlaves sets.Set[string]
	routes        []netlink.Route
}

// VRFSlaveConflictError reports that an interface is already assigned to a
// different VRF and cannot be moved without disrupting its current network.
type VRFSlaveConflictError struct {
	Interface    string
	RequestedVRF string
	ExistingVRF  string
}

func (e *VRFSlaveConflictError) Error() string {
	return fmt.Sprintf("interface %s is already assigned to VRF %s and cannot be assigned to VRF %s",
		e.Interface, e.ExistingVRF, e.RequestedVRF)
}

type Controller struct {
	mu           *sync.Mutex
	vrfs         map[int]vrf
	routeManager *routemanager.Controller
	// pendingRouteRestores holds routes whose restore failed after a master
	// change, to be retried on reconcile and on link events. The failed
	// in-memory capture is the only remaining copy of the route: the kernel
	// purged the original when the master changed.
	pendingRouteRestores []pendingRouteRestore
}

// pendingRouteRestore is a route whose restore failed, with the route's
// target table already set, and the number of restore attempts made so far.
type pendingRouteRestore struct {
	route    netlink.Route
	attempts int
}

// maxRouteRestoreAttempts bounds the retries of a failed route restore so
// that a permanently unaddable route does not stay queued forever.
const maxRouteRestoreAttempts = 10

func NewController(routeManager *routemanager.Controller) *Controller {
	return &Controller{
		mu:           &sync.Mutex{},
		vrfs:         make(map[int]vrf),
		routeManager: routeManager,
	}
}

// Run starts the VRF Manager to manage its devices
func (vrfm *Controller) Run(stopCh <-chan struct{}, doneWg *sync.WaitGroup) error {
	linkSubscribeOptions := netlink.LinkSubscribeOptions{
		ErrorCallback: func(err error) {
			klog.Errorf("Failed during LinkSubscribe callback: %v", err)
			// Note: Not calling sync() from here: it is redundant and unsafe when stopChan is closed.
		},
	}

	subscribe := func() (bool, chan netlink.LinkUpdate, error) {
		linkChan := make(chan netlink.LinkUpdate)
		if err := netlink.LinkSubscribeWithOptions(linkChan, stopCh, linkSubscribeOptions); err != nil {
			return false, nil, err
		}
		// Ensure VRFs are in sync while subscribing for Link events.
		err := vrfm.reconcile()
		if err != nil {
			klog.Errorf("VRF Manager: Error while reconciling VRFs, err: %v", err)
		}
		return true, linkChan, nil
	}
	return vrfm.runInternal(stopCh, doneWg, subscribe)
}

type subscribeFn func() (bool, chan netlink.LinkUpdate, error)

func (vrfm *Controller) runInternal(stopChan <-chan struct{}, doneWg *sync.WaitGroup,
	subscribe subscribeFn) error {
	// Get the current network namespace handle
	currentNs, err := ns.GetCurrentNS()
	if err != nil {
		return fmt.Errorf("error retrieving current net namespace, err: %v", err)
	}
	subscribed, linkUpdateCh, err := subscribe()
	if err != nil {
		if closeErr := currentNs.Close(); closeErr != nil {
			klog.Warningf("VRF Manager: failed to close namespace handle: %v", closeErr)
		}
		return fmt.Errorf("error during netlink subscribe, err: %v", err)
	}
	doneWg.Add(1)
	go func() {
		defer doneWg.Done()
		defer func() {
			if err := currentNs.Close(); err != nil {
				klog.Warningf("VRF Manager: failed to close namespace handle: %v", err)
			}
		}()
		err = currentNs.Do(func(_ ns.NetNS) error {
			linkSyncTimer := time.NewTicker(reconcilePeriod)
			defer linkSyncTimer.Stop()

			for {
				select {
				case linkUpdateEvent, ok := <-linkUpdateCh:
					linkSyncTimer.Reset(reconcilePeriod)
					if !ok {
						if subscribed, linkUpdateCh, err = subscribe(); err != nil {
							klog.Errorf("VRF Manager: Error during netlink re-subscribe due to channel closing: %v", err)
						}
						continue
					}
					ifName := linkUpdateEvent.Link.Attrs().Name
					klog.V(5).Infof("VRF Manager: link update received for interface %s", ifName)
					err = vrfm.syncVRF(linkUpdateEvent.Link)
					if err != nil {
						klog.Errorf("VRF Manager: Error syncing link %s update event, err: %v", ifName, err)
					}

				case <-linkSyncTimer.C:
					klog.V(5).Info("VRF Manager: calling reconcile() explicitly")
					if err = vrfm.reconcile(); err != nil {
						klog.Errorf("VRF Manager: Error while reconciling VRFs, err: %v", err)
					}
					if !subscribed {
						if subscribed, linkUpdateCh, err = subscribe(); err != nil {
							klog.Errorf("VRF Manager: Error during netlink re-subscribe: %v", err)
						}
					}
				case <-stopChan:
					return nil
				}
			}
		})
		if err != nil {
			klog.Errorf("VRF Manager: failed to run link reconcile goroutine, err: %v", err)
		}
	}()
	klog.Info("VRF manager is running")
	return nil
}

// reconcile synchronizes all desired VRFs and removal of stale objects
func (vrfm *Controller) reconcile() error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()
	start := time.Now()
	defer func() {
		klog.V(5).Infof("VRF Manager: reconciling VRFs took %v", time.Since(start))
	}()

	var errorAggregate []error
	validVRFDevices := make(sets.Set[string])
	for _, vrf := range vrfm.vrfs {
		validVRFDevices.Insert(vrf.name)
		err := vrfm.sync(vrf)
		if err != nil {
			errorAggregate = append(errorAggregate, fmt.Errorf("error syncing VRF %s: %v", vrf.name, err))
		}
	}

	// clean up anything stale
	if err := vrfm.repair(validVRFDevices); err != nil {
		errorAggregate = append(errorAggregate, fmt.Errorf("error repairing VRFs: %v", err))
	}

	vrfm.retryPendingRouteRestores()

	if len(errorAggregate) > 0 {
		return utilerrors.Join(errorAggregate...)
	}

	return nil
}

func (vrfm *Controller) syncVRF(link netlink.Link) error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()
	// Sustained link events keep deferring the periodic reconcile, so drain
	// queued route restores from here as well.
	defer vrfm.retryPendingRouteRestores()
	vrf, ok := vrfm.vrfs[link.Attrs().Index]
	if !ok {
		return nil
	}
	return vrfm.sync(vrf)
}

// sync ensures that the netlink VRF device exists, and the managedSlaves are
// enslaved to it. It does not handle removal of the VRF or managedSlaves,
// other than if it detects a conflict while adding.
func (vrfm *Controller) sync(vrf vrf) error {
	vrfLink, err := util.GetNetLinkOps().LinkByName(vrf.name)
	var mustRecreate bool
	if err == nil {
		vrfDev, ok := vrfLink.(*netlink.Vrf)
		if !ok {
			return fmt.Errorf("node has another non VRF device with same name %s", vrf.name)
		}
		if vrfDev.Table < uint32(config.OvnKubeNode.RoutingTableIDStart) {
			return fmt.Errorf("node has another VRF device with same name %s that is not managed by ovn-kubernetes", vrf.name)
		}
		if vrfDev.Table != vrf.table {
			klog.Warningf("Found a conflict with existing VRF device table id for VRF device %s, recreating it", vrf.name)
			err = vrfm.deleteVRF(vrfLink)
			if err != nil {
				return fmt.Errorf("failed to delete existing VRF device %s to recreate, err: %w", vrf.name, err)
			}
			mustRecreate = true
		}
	}
	// Create VRF device if it doesn't exist or if it's needed to be recreated.
	if util.GetNetLinkOps().IsLinkNotFoundError(err) || mustRecreate {
		if vrfLink != nil {
			delete(vrfm.vrfs, vrfLink.Attrs().Index)
		}
		vrfLink = &netlink.Vrf{
			LinkAttrs: netlink.LinkAttrs{Name: vrf.name},
			Table:     vrf.table,
		}
		if err = util.GetNetLinkOps().LinkAdd(vrfLink); err != nil {
			return fmt.Errorf("failed to create VRF device %s, err: %v", vrf.name, err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to retrieve existing VRF device %s, err: %v", vrf.name, err)
	}
	vrfLink, err = util.GetNetLinkOps().LinkByName(vrf.name)
	if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", vrf.name, err)
	}
	if vrfLink.Attrs().OperState != netlink.OperUp {
		if err = util.GetNetLinkOps().LinkSetUp(vrfLink); err != nil {
			return fmt.Errorf("failed to get VRF device %s operationally up, err: %v", vrf.name, err)
		}
	}
	for managedSlave := range vrf.managedSlaves {
		alreadyEnslaved, err := isInterfaceSlaveOfVRF(managedSlave, vrfLink.Attrs().Index)
		if err != nil {
			return fmt.Errorf("failed to check if %s is slave of VRF device %s, err: %v", managedSlave, vrfLink.Attrs().Name, err)
		}
		if !alreadyEnslaved {
			if err = vrfm.enslaveInterfaceToVRF(vrf.name, managedSlave, vrf.table); err != nil {
				return fmt.Errorf("failed to enslave interface %s into VRF device: %s, err: %v", managedSlave, vrf.name, err)
			}
		}
	}
	// Handover vrf routes into route manager to manage it.
	for _, route := range vrf.routes {
		if err = vrfm.routeManager.Add(route); err != nil {
			return fmt.Errorf("failed to add route %v for VRF device %s, err: %w", route, vrf.name, err)
		}
	}

	vrfm.vrfs[vrfLink.Attrs().Index] = vrf
	return nil
}

func newVRF(name string, table uint32, slaveInterface string, routes []netlink.Route) vrf {
	managedSlaves := sets.New[string]()
	if slaveInterface != "" {
		managedSlaves.Insert(slaveInterface)
	}
	return vrf{
		name:          name,
		table:         table,
		managedSlaves: managedSlaves,
		routes:        routes,
	}
}

// AddVRF adds a VRF device into the node.
func (vrfm *Controller) AddVRF(name string, slaveInterface string, table uint32, routes []netlink.Route) error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()

	if len(name) > 15 {
		return fmt.Errorf("VRF Manager: VRF name %s must be within 15 characters", name)
	}
	if table < uint32(config.OvnKubeNode.RoutingTableIDStart) {
		return fmt.Errorf("VRF Manager: cannot manage a VRF %s with table %d lower than %d", name, table, config.OvnKubeNode.RoutingTableIDStart)
	}
	var (
		vrfDev vrf
		ok     bool
	)
	vrfLink, err := util.GetNetLinkOps().LinkByName(name)
	if vrfLink != nil {
		vrfDev, ok = vrfm.vrfs[vrfLink.Attrs().Index]
		if ok {
			klog.V(5).Infof("VRF Manager: VRF %s already found in the cache", name)
			if slaveInterface != "" && !vrfDev.managedSlaves.Has(slaveInterface) {
				return fmt.Errorf("VRF Manager: slave interface mismatch for VRF device %s", name)
			}
			if vrfDev.table != table {
				return fmt.Errorf("VRF Manager: table id mismatch for VRF device %s", name)
			}
		} else {
			vrfDev = newVRF(name, table, slaveInterface, routes)
		}
	}

	if err != nil && util.GetNetLinkOps().IsLinkNotFoundError(err) {
		vrfDev = newVRF(name, table, slaveInterface, routes)
	} else if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", name, err)
	}

	return vrfm.sync(vrfDev)
}

// AddVRFSlave adds another slave interface to an existing VRF.
func (vrfm *Controller) AddVRFSlave(name string, slaveInterface string) error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()

	if slaveInterface == "" {
		return nil
	}
	vrfLink, err := util.GetNetLinkOps().LinkByName(name)
	if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", name, err)
	}
	vrfDev, ok := vrfm.vrfs[vrfLink.Attrs().Index]
	if !ok {
		return fmt.Errorf("failed to find VRF %s", name)
	}
	for _, existingVRF := range vrfm.vrfs {
		if existingVRF.name != name && existingVRF.managedSlaves.Has(slaveInterface) {
			return &VRFSlaveConflictError{
				Interface:    slaveInterface,
				RequestedVRF: name,
				ExistingVRF:  existingVRF.name,
			}
		}
	}

	slaveLink, err := util.GetNetLinkOps().LinkByName(slaveInterface)
	if err != nil {
		return fmt.Errorf("failed to retrieve slave interface %s: %v", slaveInterface, err)
	}
	if slaveLink.Attrs().MasterIndex != 0 && slaveLink.Attrs().MasterIndex != vrfLink.Attrs().Index {
		masterLink, err := util.GetNetLinkOps().LinkByIndex(slaveLink.Attrs().MasterIndex)
		if err != nil {
			return fmt.Errorf("failed to retrieve master for slave interface %s: %v", slaveInterface, err)
		}
		return &VRFSlaveConflictError{
			Interface:    slaveInterface,
			RequestedVRF: name,
			ExistingVRF:  masterLink.Attrs().Name,
		}
	}
	vrfDev.managedSlaves.Insert(slaveInterface)
	return vrfm.sync(vrfDev)
}

// DeleteVRFSlave stops managing a slave interface for an existing VRF.
func (vrfm *Controller) DeleteVRFSlave(name string, slaveInterface string) error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()

	if slaveInterface == "" {
		return nil
	}
	vrfLink, err := util.GetNetLinkOps().LinkByName(name)
	if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", name, err)
	}
	vrfDev, ok := vrfm.vrfs[vrfLink.Attrs().Index]
	if !ok {
		return fmt.Errorf("failed to find VRF %s", name)
	}
	if err = vrfm.releaseInterfaceFromVRF(slaveInterface, vrfLink.Attrs().Index, vrfDev.table, vrfDev.routes); err != nil {
		return fmt.Errorf("failed to release interface %s from VRF %s, err: %v",
			slaveInterface, name, err)
	}
	// Stop managing the slave only once released, so that a failed release
	// leaves it managed and a later retry still releases it.
	vrfDev.managedSlaves.Delete(slaveInterface)
	return vrfm.sync(vrfDev)
}

// AddVRFRoutes adds routes to the specified VRF
func (vrfm *Controller) AddVRFRoutes(name string, routes []netlink.Route) error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()

	vrfLink, err := util.GetNetLinkOps().LinkByName(name)
	if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", name, err)
	}

	vrfDev, ok := vrfm.vrfs[vrfLink.Attrs().Index]
	if !ok {
		return fmt.Errorf("failed to find VRF %s", name)
	}

	vrfDev.routes = append(vrfDev.routes, routes...)

	return vrfm.sync(vrfDev)
}

// DeleteVRFRoutes deletes a set of routes from a VRF
func (vrfm *Controller) DeleteVRFRoutes(name string, routes []netlink.Route) error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()

	vrfLink, err := util.GetNetLinkOps().LinkByName(name)
	if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", name, err)
	}

	vrf, ok := vrfm.vrfs[vrfLink.Attrs().Index]
	if !ok {
		return fmt.Errorf("failed to find VRF %s", name)
	}
	type route struct {
		LinkIndex int
		Dst       string
		Table     int
	}
	deleteRoutesRequested := sets.New[route]()
	for _, r := range routes {
		deleteRoutesRequested.Insert(route{
			LinkIndex: r.LinkIndex,
			Dst:       r.Dst.String(),
			Table:     r.Table,
		})
	}

	vrf.routes = slices.DeleteFunc(vrf.routes, func(r netlink.Route) bool {
		if err != nil {
			return false
		}
		delete := deleteRoutesRequested.Has(
			route{
				LinkIndex: r.LinkIndex,
				Dst:       r.Dst.String(),
				Table:     r.Table,
			})
		if !delete {
			return false
		}
		err = vrfm.routeManager.Del(r)
		return err == nil
	})
	vrfm.vrfs[vrfLink.Attrs().Index] = vrf
	return err
}

// Repair deletes stale VRF device(s) on the host. This helps remove
// device(s) for which DeleteVRF is never invoked.
func (vrfm *Controller) Repair(validVRFs sets.Set[string]) error {
	vrfm.mu.Lock()
	defer vrfm.mu.Unlock()

	return vrfm.repair(validVRFs)
}

func (vrfm *Controller) repair(validVRFs sets.Set[string]) error {
	links, err := util.GetNetLinkOps().LinkList()
	if err != nil {
		return fmt.Errorf("failed to list links on the node, err: %v", err)
	}

	for _, link := range links {
		vrf, isVRF := link.(*netlink.Vrf)
		if !isVRF {
			// not a vrf device
			continue
		}
		if vrf.Table < uint32(config.OvnKubeNode.RoutingTableIDStart) {
			// vrf device not managed by us
			continue
		}
		name := vrf.Name
		if validVRFs.Has(name) {
			// vrf not stale
			continue
		}
		err = vrfm.deleteVRF(link)
		if err != nil {
			klog.Errorf("VRF Manager: error deleting stale VRF device %s, err: %v", name, err)
			continue
		}
		delete(vrfm.vrfs, vrf.Index)
	}
	return nil
}

// DeleteVRF deletes given VRF device from the node.
func (vrfm *Controller) DeleteVRF(name string) (err error) {
	vrfm.mu.Lock()
	var vrfLink netlink.Link
	defer func() {
		if err == nil && vrfLink != nil {
			delete(vrfm.vrfs, vrfLink.Attrs().Index)
		}
		vrfm.mu.Unlock()
	}()
	vrfLink, err = util.GetNetLinkOps().LinkByName(name)
	if util.GetNetLinkOps().IsLinkNotFoundError(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", name, err)
	}
	vrf, ok := vrfm.vrfs[vrfLink.Attrs().Index]
	if !ok {
		klog.V(5).Infof("VRF Manager: VRF %s not found in cache for deletion", name)
		return nil
	}

	// Request route manager to delete vrf associated routes.
	for _, route := range vrf.routes {
		if err = vrfm.routeManager.Del(route); err != nil {
			return fmt.Errorf("failed to delete route %v for VRF device %s, err: %w", route, vrf.name, err)
		}
	}

	err = vrfm.deleteVRF(vrfLink)
	if err != nil {
		return fmt.Errorf("failed to delete VRF device %s, err: %w", vrf.name, err)
	}
	return nil
}

func (vrfm *Controller) deleteVRF(link netlink.Link) error {
	vrfLink, isVRF := link.(*netlink.Vrf)
	if !isVRF {
		return fmt.Errorf("node has another non VRF device with same name %s, refusing to delete it",
			link.Attrs().Name)
	}
	// Release enslaved interfaces first so that their routes are preserved
	// into the main table; deleting the VRF device would otherwise purge them
	// along with the VRF routing table. Slaves and table are derived from
	// kernel state rather than the cache, so that this also covers VRF
	// devices the cache does not know about (e.g. a stale VRF repaired after
	// a restart) or caches with a different, desired table id (recreation on
	// table conflict).
	var managedRoutes []netlink.Route
	if cached, ok := vrfm.vrfs[vrfLink.Index]; ok {
		managedRoutes = cached.routes
	}
	links, err := util.GetNetLinkOps().LinkList()
	if err != nil {
		return fmt.Errorf("failed to list links to release slaves of VRF device %s, err: %w", vrfLink.Name, err)
	}
	var errs []error
	for _, l := range links {
		if l.Attrs().MasterIndex != vrfLink.Index {
			continue
		}
		if err := vrfm.releaseInterfaceFromVRF(l.Attrs().Name, vrfLink.Index, vrfLink.Table, managedRoutes); err != nil {
			errs = append(errs, fmt.Errorf("failed to release interface %s from VRF %s, err: %w",
				l.Attrs().Name, vrfLink.Name, err))
		}
	}
	if len(errs) > 0 {
		// Do not delete the device while interfaces are still enslaved:
		// deletion would purge their routes. Callers retry the deletion.
		return utilerrors.Join(errs...)
	}
	return util.GetNetLinkOps().LinkDelete(vrfLink)
}

// isInterfaceSlaveOfVRF checks if a specific interface is enslaved to a VRF
func isInterfaceSlaveOfVRF(ifName string, vrfIndex int) (bool, error) {
	link, err := util.GetNetLinkOps().LinkByName(ifName)
	if err != nil {
		if util.GetNetLinkOps().IsLinkNotFoundError(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get link %s, err: %v", ifName, err)
	}
	return link.Attrs().MasterIndex == vrfIndex, nil
}

func (vrfm *Controller) enslaveInterfaceToVRF(vrfName, ifName string, table uint32) error {
	klog.V(5).Infof("Enslaving interface %s to VRF: %s", ifName, vrfName)
	iface, err := util.GetNetLinkOps().LinkByName(ifName)
	if err != nil {
		return fmt.Errorf("failed to retrieve interface %s, err: %v", ifName, err)
	}
	vrfLink, err := util.GetNetLinkOps().LinkByName(vrfName)
	if err != nil {
		return fmt.Errorf("failed to retrieve VRF device %s, err: %v", vrfName, err)
	}
	// Changing the interface master makes the kernel purge every FIB entry
	// referencing the interface, regenerating only local and connected routes
	// in the VRF table. Routes installed by other agents (e.g. a DHCP default
	// route on an Uplink interface) would be silently destroyed, so capture
	// them first and restore them into the VRF table after enslavement.
	routes, err := listRoutesForLink(iface, unix.RT_TABLE_MAIN)
	if err != nil {
		return fmt.Errorf("failed to list routes of interface %s before enslaving to VRF %s: %v", ifName, vrfName, err)
	}
	// Queued restores of this interface target the table it is leaving:
	// retarget them by folding them into this restore batch.
	routes = append(routes, vrfm.takePendingRouteRestores(iface.Attrs().Index)...)
	err = util.GetNetLinkOps().LinkSetMaster(iface, vrfLink)
	if err != nil {
		return fmt.Errorf("failed to enslave interface %s to VRF %s: %v", ifName, vrfName, err)
	}
	vrfm.restoreRoutesToTable(routes, table, nil)
	return nil
}

func (vrfm *Controller) releaseInterfaceFromVRF(ifName string, vrfIndex int, table uint32, ovnManagedRoutes []netlink.Route) error {
	iface, err := util.GetNetLinkOps().LinkByName(ifName)
	if err != nil {
		if util.GetNetLinkOps().IsLinkNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("failed to retrieve interface %s, err: %v", ifName, err)
	}
	if iface.Attrs().MasterIndex != vrfIndex {
		return nil
	}
	klog.V(5).Infof("Releasing interface %s from VRF", ifName)
	// Releasing the interface from the VRF purges its routes from the VRF
	// table just like enslaving purged them from the main table. Capture them
	// and restore them into the main table, except for the routes managed by
	// ovn-kubernetes itself which are handled through the route manager.
	routes, err := listRoutesForLink(iface, int(table))
	if err != nil {
		return fmt.Errorf("failed to list routes of interface %s before releasing from VRF: %v", ifName, err)
	}
	// Queued restores of this interface target the table it is leaving:
	// retarget them by folding them into this restore batch.
	routes = append(routes, vrfm.takePendingRouteRestores(iface.Attrs().Index)...)
	if err = util.GetNetLinkOps().LinkSetNoMaster(iface); err != nil {
		return err
	}
	vrfm.restoreRoutesToTable(routes, unix.RT_TABLE_MAIN, ovnManagedRoutes)
	return nil
}

// routeDstString returns a normalized destination for route comparison: a
// route dumped from the kernel carries a nil destination for a default route
// while a route built from configuration typically carries an explicit any
// CIDR, so map a nil destination to the default CIDR of the route family.
func routeDstString(route netlink.Route) string {
	if route.Dst != nil {
		return route.Dst.String()
	}
	if route.Family == netlink.FAMILY_V6 {
		return "::/0"
	}
	return "0.0.0.0/0"
}

// listRoutesForLink returns the routes of both IP families that reference the
// given link in the given routing table.
func listRoutesForLink(link netlink.Link, table int) ([]netlink.Route, error) {
	filter := &netlink.Route{LinkIndex: link.Attrs().Index, Table: table}
	return util.GetNetLinkOps().RouteListFiltered(netlink.FAMILY_ALL, filter, netlink.RT_FILTER_OIF|netlink.RT_FILTER_TABLE)
}

// shouldSkipRouteMigration returns true for routes whose owner programs them
// per routing table on its own: the kernel regenerates its routes after a
// master change, and routing daemons (FRR/zebra and friends) install and
// withdraw their routes in the routing domain they peer in, so a migrated
// copy would be a stale duplicate that no one manages.
func shouldSkipRouteMigration(route netlink.Route) bool {
	switch int(route.Protocol) {
	case unix.RTPROT_KERNEL, unix.RTPROT_ZEBRA, unix.RTPROT_BGP, unix.RTPROT_OSPF,
		unix.RTPROT_ISIS, unix.RTPROT_RIP, unix.RTPROT_EIGRP, unix.RTPROT_BABEL:
		return true
	}
	return false
}

// sameRouteKey returns true when both routes share the identity used to tell
// migrated routes apart: output interface, TOS, kernel-reported priority (an
// IPv6 route installed with no explicit priority gets the kernel user
// default, so a configured priority of 0 and a dumped 1024 must compare
// equal), gateway and normalized destination.
func sameRouteKey(a, b netlink.Route) bool {
	return a.LinkIndex == b.LinkIndex &&
		a.Tos == b.Tos &&
		routemanager.KernelRoutePriority(&a) == routemanager.KernelRoutePriority(&b) &&
		a.Gw.Equal(b.Gw) &&
		routeDstString(a) == routeDstString(b)
}

// restoreRoutesToTable re-adds routes into the given routing table, preserving
// all their attributes. Routes that their owner reprograms per routing table
// are skipped, and so are the excluded routes. Failure to restore a route is
// logged and the route is queued for retry on reconcile: the enslavement
// itself is not retried since it already succeeded, and the in-memory capture
// is the only remaining copy of the route.
func (vrfm *Controller) restoreRoutesToTable(routes []netlink.Route, table uint32, excluded []netlink.Route) {
	isExcluded := func(route netlink.Route) bool {
		for _, excludedRoute := range excluded {
			if sameRouteKey(route, excludedRoute) {
				return true
			}
		}
		return false
	}
	for _, route := range sortRoutesForRestore(routes) {
		if shouldSkipRouteMigration(route) || isExcluded(route) {
			continue
		}
		route.Table = int(table)
		// Keep only the configuration flags relevant to route installation:
		// the kernel rejects requests that carry its own state flags, e.g.
		// RTNH_F_LINKDOWN captured from a carrier-down interface.
		route.Flags &= unix.RTNH_F_ONLINK
		if done, err := tryRestoreRoute(route); !done {
			klog.Warningf("VRF Manager: failed to restore route %v into table %d, will retry: %v", route, table, err)
			vrfm.queueRouteRestore(route)
		}
	}
}

// sortRoutesForRestore orders routes without a gateway first: a gateway route
// is only accepted once the route covering its gateway is in place.
func sortRoutesForRestore(routes []netlink.Route) []netlink.Route {
	routes = slices.Clone(routes)
	slices.SortStableFunc(routes, func(a, b netlink.Route) int {
		gatewayRoutes := func(r netlink.Route) int {
			if len(r.Gw) > 0 {
				return 1
			}
			return 0
		}
		return gatewayRoutes(a) - gatewayRoutes(b)
	})
	return routes
}

// tryRestoreRoute adds the route into its table. It returns true when there
// is nothing left to do for the route: it was added, or its slot in the table
// is already occupied, in which case adding is impossible and replacing could
// clobber a route we do not own.
func tryRestoreRoute(route netlink.Route) (bool, error) {
	err := addRouteWithRetry(&route)
	if err == nil {
		klog.V(5).Infof("VRF Manager: restored route %v into table %d", route, route.Table)
		return true, nil
	}
	if util.GetNetLinkOps().IsAlreadyExistsError(err) {
		klog.V(5).Infof("VRF Manager: not restoring route %v into table %d: its slot is already occupied", route, route.Table)
		return true, nil
	}
	return false, err
}

// queueRouteRestore remembers a route whose restore failed so that it is
// retried later, keeping the latest capture when an entry with the same
// identity is already queued. Expiring routes are not queued: their owner
// refreshes them, and re-adding one later would wrongly extend its lifetime.
// Must be called with the controller mutex held.
func (vrfm *Controller) queueRouteRestore(route netlink.Route) {
	if route.Expires > 0 {
		klog.Warningf("VRF Manager: not queueing restore of expiring route %v", route)
		return
	}
	vrfm.pendingRouteRestores = slices.DeleteFunc(vrfm.pendingRouteRestores, func(pending pendingRouteRestore) bool {
		return pending.route.Table == route.Table && sameRouteKey(pending.route, route)
	})
	vrfm.pendingRouteRestores = append(vrfm.pendingRouteRestores, pendingRouteRestore{route: route})
}

// takePendingRouteRestores removes and returns the queued routes of the given
// interface. The enslave and release paths call it before changing the
// interface's master: the queued routes target the routing table the
// interface is leaving, so the caller folds them into its own restore batch
// toward the new table instead of letting a later retry install them into a
// table the interface is no longer part of. Must be called with the
// controller mutex held.
func (vrfm *Controller) takePendingRouteRestores(linkIndex int) []netlink.Route {
	var taken []netlink.Route
	vrfm.pendingRouteRestores = slices.DeleteFunc(vrfm.pendingRouteRestores, func(pending pendingRouteRestore) bool {
		if pending.route.LinkIndex != linkIndex {
			return false
		}
		taken = append(taken, pending.route)
		return true
	})
	return taken
}

// retryPendingRouteRestores re-attempts route restores that failed, e.g.
// because of a transient netlink error. An entry is dropped once there is
// nothing left to do for it, when the interface it references is gone, at
// which point the route is unrecoverable, or after too many attempts. Must be
// called with the controller mutex held.
func (vrfm *Controller) retryPendingRouteRestores() {
	if len(vrfm.pendingRouteRestores) == 0 {
		return
	}
	// gateway routes last, as in the original restore
	slices.SortStableFunc(vrfm.pendingRouteRestores, func(a, b pendingRouteRestore) int {
		gatewayRoutes := func(p pendingRouteRestore) int {
			if len(p.route.Gw) > 0 {
				return 1
			}
			return 0
		}
		return gatewayRoutes(a) - gatewayRoutes(b)
	})
	kept := vrfm.pendingRouteRestores[:0]
	for _, pending := range vrfm.pendingRouteRestores {
		done, err := tryRestoreRoute(pending.route)
		if done {
			continue
		}
		if _, linkErr := util.GetNetLinkOps().LinkByIndex(pending.route.LinkIndex); linkErr != nil &&
			util.GetNetLinkOps().IsLinkNotFoundError(linkErr) {
			klog.Warningf("VRF Manager: dropping restore of route %v: its interface is gone", pending.route)
			continue
		}
		pending.attempts++
		if pending.attempts >= maxRouteRestoreAttempts {
			klog.Errorf("VRF Manager: giving up on restoring route %v into table %d after %d attempts, last error: %v",
				pending.route, pending.route.Table, pending.attempts, err)
			continue
		}
		klog.Warningf("VRF Manager: failed to restore route %v into table %d, will retry: %v",
			pending.route, pending.route.Table, err)
		kept = append(kept, pending)
	}
	vrfm.pendingRouteRestores = kept
}

const (
	// routeRestoreTimeout bounds how long a route restore is retried while
	// the kernel deems the route's gateway unreachable. It has to cover
	// duplicate address detection (about a second with the default
	// dad_transmits) plus the addrconf work queue latency, with headroom for
	// loaded nodes.
	routeRestoreTimeout      = 4 * time.Second
	routeRestorePollInterval = 100 * time.Millisecond
)

// addRouteWithRetry adds the route, retrying briefly while the kernel deems
// its gateway unreachable: IPv6 connected routes are regenerated
// asynchronously after a master change, so a gateway route restored right
// after it can transiently fail the kernel's reachability validation.
func addRouteWithRetry(route *netlink.Route) error {
	var err error
	_ = wait.PollUntilContextTimeout(context.Background(), routeRestorePollInterval, routeRestoreTimeout, true,
		func(context.Context) (bool, error) {
			err = util.GetNetLinkOps().RouteAdd(route)
			return err == nil || !(errors.Is(err, unix.EHOSTUNREACH) || errors.Is(err, unix.ENETUNREACH)), nil
		})
	return err
}
