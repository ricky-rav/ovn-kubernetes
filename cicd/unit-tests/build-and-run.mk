#
# Build and run unit tests
#

SHELL := /bin/bash

group-no ?= 1
groups-total ?= 1

controller-github := github.com/ovn-org/ovn-kubernetes/go-controller

$(if $(controller-dir),$(info Controller dir: $(controller-dir)),$(error controller-dir must be defined))
$(if $(build-dir),$(info Build-dir: $(build-dir)),$(error build-dir must be defined))
$(if $(tests-paths),$(info Paths: $(tests-paths)),$(error tests-paths are not resolved))
$(if $(root-packages),$(info Root packages: $(root-packages)),$(error root-packages are not resolved))

go-get-test-pkg = $(shell cd $1 && go list -buildvcs=false -mod vendor -f '{{if len .TestGoFiles}} {{.Dir}} {{end}}' $2 | sort | xargs)

# Short representation of package like: pkg/ovn pkg/controllermanager
all-packages-short := $(subst $(controller-dir)/,,$(call go-get-test-pkg,$(controller-dir),$(tests-paths)))

# Short representation of packages requires root:
root-packages-short := $(subst $(controller-github)/,,$(root-packages))

$(if $(all-packages-short),$(info All packages: $(all-packages-short)),$(error Cannot get all packages))
$(if $(root-packages-short),$(info Root packages: $(root-packages-short)),$(error Cannot get root packages))

# Packages that can be run using ginkgo parallel:
parallel-run = pkg/ovn \
               pkg/ovn/address_set \
               pkg/clustermanager \
               pkg/clustermanager/status_manager/zone_tracker \
               pkg/clustermanager/endpointslicemirror \
               pkg/factory \
               pkg/informer \
               pkg/config

# Package that has longest runtime (will be run in separate group if groups-total > 1)
dominant-pkg = $(filter pkg/ovn,$(all-packages-short))
other-pkgs = $(filter-out $(dominant-pkg),$(all-packages-short))

$(if $(dominant-pkg),,$(error Cannot detect dominant package))

is-single-group := $(filter 1,$(groups-total))

n-packages := $(words $(other-pkgs))
n-groups := $(shell expr $(groups-total) - 2)
groups-size := $(if $(is-single-group),,$(shell expr $(n-packages) / $(n-groups)))

$(info total packages: $(n-packages))
$(if $(is-single-group),,$(info group size packages: $(groups-size)))

single-group := $(all-packages-short)

# First group is dominant-pkg
# Last group contains all packages including reminder of division
# Middle group contains packages in specific range
#
is-first-group = $(filter 1,$(group-no))
is-last-group = $(filter $(groups-total),$(group-no))

multi-group-split = $(if $(is-first-group),$(dominant-pkg),$(non-first-group))
non-first-group = $(wordlist $(group-start),$(group-end),$(other-pkgs))
group-start = $(shell expr '(' $(group-no) - 2 ')' '*' $(groups-size) + 1)
group-end = $(if $(is-last-group),$(n-packages),$(shell expr '(' $(group-no) - 1 ')' '*' $(groups-size)))

current-group = $(if $(is-single-group),$(single-group),$(multi-group-split))

$(if $(is-single-group)$(is-first-group),,$(info Current group size: $(words $(current-group)) starts: $(group-start); ends: $(group-end)))
$(if $(is-first-group),$(info This is first group: $(current-group)))


current-group-tagets = $(foreach pkg,$(current-group),$(call test-target,$(pkg)))

make-name = $(subst /,_,$1)
test-target = $(call make-name,$1)-ci-check
test-binary = $(build-dir)/$(call make-name,$1).test
junit-report = junit-$(call make-name,$1).xml
coverprofile = $(call make-name,$1).coverprofile
test-log = $(build-dir)/$(call make-name,$1).log

all: $(current-group-tagets)

# This is ugly ginkgo test same as in hack/test-go.sh. It is better to replace
# it with something more stable.
is-ginkgo-package = $(shell grep ginkgo $(controller-dir)/$1/*_test.go)

define define-package-rules
$(call test-binary,$1):
	$(call build-test-binary,$1,$$@)

.PHONY: $(call test-target,$1)
$(call test-target,$1): $(call test-binary,$1)
	$(call run-test-binary,$1)

endef

build-test-binary = $(if $(is-ginkgo-package),$(call build-ginkgo-test-binary,$1),$(call build-go-test-binary,$1))
run-test-binary = $(if $(is-ginkgo-package),$(call run-ginkgo-test-binary,$1),$(call run-go-test-binary,$1))

# TODO: ugly mv *.test $$@ maybe replaced with -o $$@ when newef ginkgo will be introduced (2.23.4 or later)
define build-ginkgo-test-binary
	@echo Build ginkgo $1 $$@
	cd $(controller-dir)/$1 && ginkgo build --mod vendor --covermode atomic --race && mv *.test $$@

endef

define build-go-test-binary
	@echo Build go test $1 $$@
	cd $(controller-dir) && go test -buildvcs=false -mod=vendor -covermode atomic -race -c $(controller-github)/$1 -o $$@

endef


check-requires-root = $(filter $1,$(root-packages-short))
check-parallel = $(filter $1,$(parallel-run))
maybe-add-sudo = $(if $(call check-requires-root,$1),sudo )
maybe-add-parallel = $(if $(call check-parallel,$1),--output-interceptor-mode none --nodes 6)

# Suppress log output of test by filtering
suppress-logs = | grep -v '^[ ]*[ID][0-9]*' | grep -v '^[ ]*[0-9]*/[0-9]*/[0-9]*'

tee-logs = | tee >(gzip --stdout > $(call test-log,$1).gz)

run-ginkgo-test-binary = cd $(controller-dir) && $(call maybe-add-sudo,$1)ginkgo \
                             run \
                             -v \
                             --force-newlines \
                             --output-dir $(build-dir) \
                             --junit-report $(call junit-report,$1) \
                             --coverprofile $(call coverprofile,$1) \
                             --flake-attempts 4                     \
                             $(call maybe-add-parallel,$1)          \
                             $(call test-binary,$1) 2>&1 \
                             $(call tee-logs,$1) $(suppress-logs)

run-go-test-binary = $(call maybe-add-sudo,$1)$(call test-binary,$1) \
                            --test.v \
                            -test.coverprofile $(build-dir)/$(call coverprofile,$1) 2>&1 \
                            $(call tee-logs,$1) $(suppress-logs)

$(foreach pkg,$(current-group),$(eval $(call define-package-rules,$(pkg))))
