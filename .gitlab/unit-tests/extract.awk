#
# Compilation of go-controller/hack/test-go.sh
# To include file for make that contains following variables:
#
# tests-paths - list of paths that contains packages with unit test:
# root-packages - list of packages requires root
#
# For today:
# tests-paths = ./cmd/... ./pkg/... ./hybrid-overlay/...
# root-packages = github.com/ovn-org/ovn-kubernetes/go-controller/pkg/controllermanager \
#            github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node \
#            github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/iptables
#            github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/rulemanager \
#            github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/routemanager \
#            github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/vrfmanager \
#            github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/controllers/egressip
#
#

# Extract paths to unit tests from string:
# PKGS=$(gocmd list -mod vendor -f '{{if len .TestGoFiles}} {{.ImportPath}} {{end}}' ${PKGS:-./cmd/... ./pkg/... ./hybrid-overlay/...} | xargs)
/^PKGS=/ {
    if (packages_found) {
        next
    }
    pattern = $0
    paths = gensub(/^PKGS=.*[$]{PKGS:-([^}]*).*/, "\\1", "g", pattern)
    if (pattern != paths) {
        packages_found = 1;
        print "tests-paths = " paths
    }
}

# Extract paths to unit tests from string:
# PKGS=$(gocmd list -mod vendor -f '{{if len .TestGoFiles}} {{.ImportPath}} {{end}}' ${PKGS:-./cmd/... ./pkg/... ./hybrid-overlay/...} | xarg)
/^root_pkgs=/ {
    if (root_packages_found) {
        next
    }
    pattern = $0
    root_packages = gensub(/^root_pkgs=\(([^)]*)\)/, "\\1", "g", pattern)
    root_packages = gensub(/"/, "", "g", root_packages)
    if (pattern != root_packages) {
        root_packages_found = 1;
        print "root-packages = " root_packages
    }
}

// {}

END {
    if (!packages_found) {
        print "Packages are not found in file" > "/dev/stderr"
        exit 1
    }
    if (!root_packages_found) {
        print "Root packages are not found in file" > "/dev/stderr"
        exit 1
    }
}
