#
# Common functions
#

# ================================================================================
# Image names generation
#
# $1 - Linux distrib (ubuntu / rocky / ...)
# Examples:
#    $(call make-ovn-c-image,$(dist))
#    $(call make-ovnkube-c-image,$(dist))
#
# Historically ubuntu-arm64 were used for DPU (and called ovn-c-arm64) 
make-ovn-c-image-ubuntu = ovn-c-arm64
make-ovnkube-c-image-ubuntu = ovnkube-c-arm64

make-ovn-c-image-rocky  = ovn-c
make-ovnkube-c-image-rocky = ovnkube-c

make-ovn-c-image = $(call make-ovn-c-image-$1)
make-ovnkube-c-image = $(call make-ovnkube-c-image-$1)

# ================================================================================
# Image tag generation
#
# $1 - Linux distrib (ubuntu / rocky / ...)
# $2 - Target platform (arm64 / amd64)
# $3 - Base image tag (00b64f9c-21996998)
# Example: $(call make-image-tag,$(dist),$(platform),$(image-tag))
#
make-image-tag-ubuntu = $2
make-image-tag-rocky = $1.$2

make-image-tag = $(call make-image-tag-$1,$2,$3)


