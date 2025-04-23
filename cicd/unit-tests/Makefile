#
# Prepare variabled to build-and-run.mk
#

group-no ?= 1
groups-total ?= 1

$(if $(controller-dir),$(info Controller dir: $(controller-dir)),$(error controller-dir must be defined))
$(if $(build-dir),$(info Build-dir: $(build-dir)),$(error build-dir must be defined))

hacks-test-sh = $(controller-dir)/hack/test-go.sh
extracted-vars = $(build-dir)/make-and-check-extracted-vars.mk

-include $(extracted-vars)

all:
	make -f ./build-and-run.mk --warn-undefined-variables \
              tests-paths="$(tests-paths)" \
              root-packages="$(root-packages)" \
              controller-dir=$(controller-dir) \
              build-dir=$(build-dir) \
              group-no=$(group-no) \
              groups-total=$(groups-total) \


$(extracted-vars): $(hacks-test-sh) ./unfold.awk ./extract.awk
	awk -f ./unfold.awk $< | awk -f ./extract.awk > $@.tmp
	mv $@.tmp $@
