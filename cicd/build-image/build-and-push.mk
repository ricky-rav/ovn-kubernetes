#
# Build container images
#

$(if $(platform),$(info Platform: $(platform)),$(error platform is not set))
$(if $(dist),$(info Distrib: $(dist)),$(error dist is not set))
$(if $(registry-tag-path),$(info Registry tag path: $(registry-tag-path)),$(error registry-tag-path is not set))
$(if $(image-tag),$(info Image tag: $(image-tag)),$(error image-tag is not set))

ifeq ($(filter tag-latest,$(MAKECMDGOALS)),tag-latest)
$(if $(latest-tag),$(info Latest tag: $(latest-tag)),$(error latest-tag is not set fot tag-latest goal))
else
$(if $(k8s-version),$(info Kubernetes version: $(k8s-version)),$(error k8s-version is not set))
$(if $(ovn-version),$(info OVN version: $(ovn-version)),$(error ovn-version is not set))
$(if $(ovs-version),$(info OVS version: $(ovs-version)),$(error ovs-version is not set))
$(if $(artifactory-creds),$(info Artificatory creds are set),$(error artifactory-creds is not set))
endif

dry-run ?=
ifeq ($(dry-run),true)
$(info WARNING: dry-run mode)
endif

include functions.mk

#
ovn-c-image = $(registry-tag-path)/$(call make-ovn-c-image,$(dist)):$(call make-image-tag,$(dist),$(platform),$(image-tag))
ovnkube-c-image = $(registry-tag-path)/$(call make-ovnkube-c-image,$(dist)):$(call make-image-tag,$(dist),$(platform),$(image-tag))

ifeq ($(filter tag-latest,$(MAKECMDGOALS)),tag-latest)
ovn-c-latest-image = $(registry-tag-path)/$(call make-ovn-c-image,$(dist)):$(call make-image-tag,$(dist),$(platform),$(latest-tag))
ovnkube-c-latest-image = $(registry-tag-path)/$(call make-ovnkube-c-image,$(dist)):$(call make-image-tag,$(dist),$(platform),$(latest-tag))
endif


ifeq ($(dist),ubuntu)
# For ubuntu linux we don't have stages in Dockerfile and both images are the same
dockerfile-is-multistage =
dockerfile = Dockerfile.ubuntu.arm64
endif

ifeq ($(dist),rocky)
# For rocky linux we have ovn and ovnkube stages in Dockerfile and can use them to reduce size
dockerfile-is-multistage = true
dockerfile = Dockerfile
endif

ifndef ovn-c-image
$(error Failed to caclculate ovn-c image name)
endif

ifndef ovnkube-c-image
$(error Failed to caclculate ovnkube-c image name)
endif

ifndef dockerfile
$(error Failed to caclculate dockerfile name)
endif

build-args := K8S_VER=$(k8s-version) \
	      OVS_VER=$(ovs-version) \
              OVN_VER=$(ovn-version) \
              ARCH=$(platform) \
              ARTIFACTORY_CERT=${artifactory-creds}
docker-args = --no-cache \
              --network=host \
              $(addprefix --build-arg ,$(build-args)) \
              -f $(dockerfile)

ifeq ($(dry-run),true)
docker = @echo "DRY RUN: cd ../../dist/images && docker"
else
docker = cd ../../dist/images && docker
endif


ifeq ($(dockerfile-is-multistage),true)
define build-containers
	$(docker) build $(docker-args) --target ovnkube -t $(ovnkube-c-image) .
	$(docker) build $(docker-args) --target ovn -t $(ovn-c-image) .

endef

else
define build-containers
	$(docker) build $(docker-args) -t $(ovnkube-c-image) .
	$(docker) tag $(ovnkube-c-image) $(ovn-c-image)

endef

endif

$(info ovn-c container image: $(ovn-c-image))
$(info ovnkube-c container image: $(ovn-c-image))

all:
	$(call build-containers)
	$(docker) push $(ovn-c-image)
	$(docker) push $(ovnkube-c-image)

tag-latest:
	$(if $(ovn-c-latest-image),$(info Tagging ovn-c $(ovn-c-image) with $(ovn-c-latest-image)),$(error ovn-c-latest-image is not defined))
	$(if $(ovnkube-c-latest-image),$(info Tagging ovn-c $(ovnkube-c-image) with $(ovnkube-c-latest-image)),$(error ovnkube-c-latest-image is not defined))
	$(docker) pull $(ovn-c-image)
	$(docker) pull $(ovnkube-c-image)
	$(docker) tag $(ovn-c-image) $(ovn-c-latest-image)
	$(docker) tag $(ovnkube-c-image) $(ovnkube-c-latest-image)
	$(docker) push $(ovn-c-latest-image)
	$(docker) push $(ovn-c-latest-image)
