#
# Build multiarch container image
#

$(if $(dist),$(info Distrib: $(dist)),$(error dist is not set))
$(if $(registry-tag-path),$(info Registry tag path: $(registry-tag-path)),$(error registry-tag-path is not set))
$(if $(image-tag),$(info Image tag: $(image-tag)),$(error image-tag is not set))
$(if $(platforms),$(info Platforms: $(addsuffix ;,$(platforms))),$(error platforms is not set))

include functions.mk

ovn-c-image = $(registry-tag-path)/$(call make-ovn-c-image,$(dist)):$(image-tag)
ovnkube-c-image = $(registry-tag-path)/$(call make-ovnkube-c-image,$(dist)):$(image-tag)

$(info multiarch ovn-c image: $(ovn-c-image))
$(info multiarch ovnkube-c image: $(ovnkube-c-image))

docker = docker

# $1 is ovnkube-c or ovn-c
# $2 is platform
make-full-arch-image-tag = $(registry-tag-path)/$(call make-$1-image,$(dist)):$(call make-image-tag,$(dist),$2,$(image-tag))

# $1 is ovnkube-c or ovn-c
define build-multiarch
	$(docker) manifest create $($1-image) $(foreach platform,$(platforms),--amend $(call make-full-arch-image-tag,$1,$(platform)))
	$(docker) manifest push $($1-image)

endef


all:
	$(foreach x,ovnkube-c ovn-c,$(call build-multiarch,$(x)))



