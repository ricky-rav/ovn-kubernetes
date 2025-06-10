#
# Build container images
#

$(if $(platform),$(info Platform: $(platform)),$(error platform is not set))
$(if $(dist),$(info Distrib: $(dist)),$(error dist is not set))
$(if $(registry-tag-path),$(info Registry tag path: $(registry-tag-path)),$(error registry-tag-path is not set))
$(if $(image-tag),$(info Image tag: $(image-tag)),$(error image-tag is not set))

dry-run ?=
ifeq ($(dry-run),true)
$(info WARNING: dry-run mode)
endif

include functions.mk

#
netprobes-image = $(registry-tag-path)/$(call make-netprobes-image,$(dist)):$(call make-image-tag,$(dist),$(platform),$(image-tag))
dockerfile = Dockerfile.networkprobe-target

ifndef netprobes-image
$(error Failed to caclculate netprobes-image image name)
endif

ifndef dockerfile
$(error Failed to caclculate dockerfile name)
endif

docker-args = --no-cache \
              --network=host \
              -f $(dockerfile)

ifeq ($(dry-run),true)
docker = @echo "DRY RUN: cd ../../dist/images && docker"
else
docker = cd ../../dist/images && docker
endif

define build-containers
	$(docker) build $(docker-args) -t $(netprobes-image) .

endef

$(info netprobes-image container image: $(netprobes-image))

all:
	$(call build-containers)
	$(docker) push $(netprobes-image)
