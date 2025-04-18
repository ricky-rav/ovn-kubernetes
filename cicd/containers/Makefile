#
# Build and publish docker images required to build
#

$(if $(platform),$(info Building for platform $(platform)),$(error platform is not set))
$(if $(dist),$(info Building for dist $(dist)),$(error dist is not set))
$(if $(dist_version),$(info Building for dist version: $(dist_version)),$(error dist_version is not set))

image-name = builder-$(dist)$(dist_version)-$(platform)
docker-file = $(dist)$(dist_version)-$(platform).dockerfile

# Check prerequisite variable are set:
gitlab-api-v4 = $(if $(CI_API_V4_URL),$(CI_API_V4_URL),$(error CI_API_V4_URL is not set))
gitlab-api-token = $(if $(GITLAB_API_TOKEN),$(GITLAB_API_TOKEN),$(error GITLAB_API_TOKEN is not set))
gitlab-project-id = $(if $(CI_PROJECT_ID),$(CI_PROJECT_ID),$(error CI_PROJECT_ID is not set))
gitlab-registry = $(if $(CI_REGISTRY),$(CI_REGISTRY),$(error CI_REGISTRY is not set))
gitlab-project-path = $(if $(CI_PROJECT_PATH),$(CI_PROJECT_PATH),$(error CI_PROJECT_PATH is not set))

# Extract repositry ID from the project ID that related to $(image-name):
gitlab-jq-extract-repo-id = '.[] | select(."name" == "$(image-name)") | .id'
gitlab-api-path-repositories = $(gitlab-api-v4)/projects/$(gitlab-project-id)/registry/repositories
$(info Registry repository request: $(shell curl --header "PRIVATE-TOKEN: $(gitlab-api-token)" $(gitlab-api-path-repositories)))
maybe-gitlab-registry-id := $(shell curl --header "PRIVATE-TOKEN: $(gitlab-api-token)" $(gitlab-api-path-repositories) | jq -r $(gitlab-jq-extract-repo-id))

# Check that only zero or one registry has name $(image-name)
zero-or-one = $(if $1,$(if $(filter 1,$(words $1)),true),true)
$(if $(call zero-or-one,$(maybe-gitlab-registry-id)),,$(error Ambiguous registry with name: $(image-name): $(maybe-gitlab-registry-id)))

$(info Project registry id for $(image-name): $(maybe-gitlab-registry-id))

gitlab-api-path-image-tags = $(gitlab-api-path-repositories)/$(maybe-gitlab-registry-id)/tags
gitlab-jq-extract-image-tags = '.[] | ."name"'
gitlab-image-tags := $(if $(maybe-gitlab-registry-id),$(shell curl --header "PRIVATE-TOKEN: $(gitlab-api-token)" '$(gitlab-api-path-image-tags)' | jq -r $(gitlab-jq-extract-image-tags) | xargs))

$(info Tags of images: $(gitlab-image-tags))

# $(call dep-sha256sum,$1) produces sha256 sum of $1
dep-sha256sum = $(shell echo $1 | sha256sum | cut -f1 -d ' ')
# $(call cat-sha256sum,$1) produces sha256 sum of content of file $1
cat-sha256sum = $(shell cat $1 | sha256sum | cut -f1 -d ' ')

# Name of target tag of image with name $(image-name)
target-tag := $(call dep-sha256sum,$(call cat-sha256sum, $(docker-file)))
$(info target image tag: $(target-tag))

image-full-tag = $(gitlab-registry)/$(gitlab-project-path)/$(image-name):$(target-tag)
$(info target image full tag: $(image-full-tag))

registry-dir = _registry

.NOTPARALLEL:
.PHONY: all
all: registry $(registry-dir)/$(target-tag)
	echo $(image-full-tag) > $(registry-dir)/current-tag

$(registry-dir)/$(target-tag): $(docker-file)
	docker build -f $(docker-file) -t $(image-full-tag) .
	docker push $(image-full-tag)

# Create fake files with names same as tags inside registry dir:
define create-image-fake-file
	touch $(registry-dir)/$1

endef

.PHONY: registry
registry: $(registry-dir)/.stamp
	$(foreach tag,$(gitlab-image-tags),$(call create-image-fake-file,$(tag)))

$(registry-dir)/.stamp:
	mkdir $(dir $@)
	touch $@
