#
# Build multiarch container image
#

$(if $(dist),$(info Distrib: $(dist)),$(error dist is not set))
$(if $(registry-tag-path),$(info Registry tag path: $(registry-tag-path)),$(error registry-tag-path is not set))
$(if $(image-tag),$(info Image tag: $(image-tag)),$(error image-tag is not set))
$(if $(platforms),$(info Platforms: $(addsuffix ;,$(platforms))),$(error platforms is not set))

dry-run ?=
ifeq ($(dry-run),true)
$(info WARNING: dry-run mode)
endif

include functions.mk

netprobes-image = $(registry-tag-path)/$(call make-netprobes-image,$(dist)):$(image-tag)

$(info multiarch netprobes image: $(netprobes-image))

ifeq ($(dry-run),true)
docker = @echo "DRY RUN: docker"
else
docker = docker
endif

# $1 is platform
make-full-arch-image-tag = $(registry-tag-path)/$(call make-netprobes-image,$(dist)):$(call make-image-tag,$(dist),$1,$(image-tag))

# $1 is ovnkube-c or ovn-c
define build-multiarch

endef

.PHONY: all
all:
	$(docker) manifest create $(netprobes-image) $(foreach platform,$(platforms),--amend $(call make-full-arch-image-tag,$(platform)))
	$(docker) manifest push $(netprobes-image)
