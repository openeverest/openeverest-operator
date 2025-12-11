# VERSION defines the project version for the bundle.
# Update this value when you upgrade the version of your project.
# To re-generate a bundle for another specific version without changing the standard setup, you can:
# - use the VERSION as arg of the bundle target (e.g make bundle VERSION=0.0.2)
# - use environment variables to overwrite this value (e.g export VERSION=0.0.2)
VERSION ?= 0.0.0

FLAGS = -X 'github.com/percona/everest-operator/internal/consts.Version=$(VERSION)'
LD_FLAGS_OPERATOR = -ldflags " $(FLAGS)"

# CHANNELS define the bundle channels used in the bundle.
# Add a new line here if you would like to change its default config. (E.g CHANNELS = "candidate,fast,stable")
# To re-generate a bundle for other specific channels without changing the standard setup, you can:
# - use the CHANNELS as arg of the bundle target (e.g make bundle CHANNELS=candidate,fast,stable)
# - use environment variables to overwrite this value (e.g export CHANNELS="candidate,fast,stable")
ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif

# DEFAULT_CHANNEL defines the default channel used in the bundle.
# Add a new line here if you would like to change its default config. (E.g DEFAULT_CHANNEL = "stable")
# To re-generate a bundle for any other default channel without changing the default setup, you can:
# - use the DEFAULT_CHANNEL as arg of the bundle target (e.g make bundle DEFAULT_CHANNEL=stable)
# - use environment variables to overwrite this value (e.g export DEFAULT_CHANNEL="stable")
ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

# IMAGE_TAG_BASE defines the docker.io namespace and part of the image name for remote images.
# This variable is used to construct full image tags for bundle and catalog images.
#
# For example, running 'make bundle-build bundle-push catalog-build catalog-push' will build and push both
# percona.com/everest-operator-bundle:$VERSION and percona.com/everest-operator-catalog:$VERSION.
IMAGE_TAG_OWNER ?= docker.io/perconalab
IMAGE_TAG_BASE ?= $(IMAGE_TAG_OWNER)/everest-operator

# BUNDLE_IMG defines the image:tag used for the bundle.
# You can use it as an arg. (E.g make bundle-build BUNDLE_IMG=<some-registry>/<project-name-bundle>:<tag>)
BUNDLE_IMG ?= $(IMAGE_TAG_BASE)-bundle:$(VERSION)

# BUNDLE_GEN_FLAGS are the flags passed to the operator-sdk generate bundle command
BUNDLE_GEN_FLAGS ?= -q --overwrite --version $(VERSION) $(BUNDLE_METADATA_OPTS)

# USE_IMAGE_DIGESTS defines if images are resolved via tags or digests
# You can enable this value if you would like to use SHA Based Digests
# To enable set flag to true
USE_IMAGE_DIGESTS ?= false
ifeq ($(USE_IMAGE_DIGESTS), true)
	BUNDLE_GEN_FLAGS += --use-image-digests
endif

# Image URL to use all building/pushing image targets
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

OS=$(shell go env GOHOSTOS)
ARCH=$(shell go env GOHOSTARCH)
CWD=$(shell pwd)

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
#SHELL = /usr/bin/env bash -o pipefail
#.SHELLFLAGS = -ec

.PHONY: all
all: help

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: init
init: cleanup-localbin  ## Install development tools
	$(MAKE) kustomize
	$(MAKE) controller-gen
	$(MAKE) operator-sdk
	$(MAKE) opm
	$(MAKE) kubebuilder

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: cleanup-localbin
cleanup-localbin:
	@echo "Cleaning up local bin directory..."
	rm -rf $(LOCALBIN)

.PHONY: format
format: ## Format code with gofmt, goimports, and gci.
	GOOS=$(OS) GOARCH=$(ARCH) go tool gofumpt -l -w .
	GOOS=$(OS) GOARCH=$(ARCH) go tool goimports -local github.com/percona/everest-operator -l -w .
	GOOS=$(OS) GOARCH=$(ARCH) go tool gci write --skip-generated -s standard -s default -s "prefix(github.com/percona/everest-operator)" .

.PHONY: static-check
static-check: ## Run static checks.
	go tool golangci-lint run --config=./.golangci.yml --new-from-rev=$(shell git merge-base main HEAD) --fix
	go tool license-eye -c .licenserc.yaml header fix

.PHONY: build
build: $(LOCALBIN) manifests generate format ## Build manager binary.
	go build -o $(LOCALBIN)/manager cmd/main.go
	go build -o $(LOCALBIN)/data-importer internal/data-importer/main.go

.PHONY: build-debug
build-debug: $(LOCALBIN) manifests generate format ## Build manager binary with debug symbols.
	CGO_ENABLED=0 go build -gcflags 'all=-N -l' -o $(LOCALBIN)/manager cmd/main.go

.PHONY: run
run: manifests generate format ## Run a controller from your host.
	go run ./cmd/main.go

.PHONY: local-env-up
minikube-cluster-up: ## Create a local minikube cluster
	minikube start --nodes=3 --cpus=4 --memory=4g --apiserver-names host.docker.internal
	minikube addons disable storage-provisioner
	kubectl delete storageclass standard
	kubectl apply -f ./dev/kubevirt-hostpath-provisioner.yaml

.PHONY: prepare-pr
prepare-pr: manifests generate static-check format bundle build ## Prepare the code for creating PR.

##@ Testing

.PHONY: test
test: ## Run unit tests.
	go test ./...

.PHONY: test-integration-core
test-integration-core: docker-build k3d-upload-image ## Run integration/core tests against K8S cluster
	. ./test/vars.sh && kubectl kuttl test --config ./test/integration/kuttl-core.yaml

.PHONY: test-integration-features
test-integration-features: docker-build k3d-upload-image ## Run feature tests against K8S cluster
	. ./test/vars.sh && kubectl kuttl test --config ./test/integration/kuttl-features.yaml

.PHONY: test-integration-engine-features
test-integration-engine-features: docker-build k3d-upload-image ## Run engine-features tests against K8S cluster
	. ./test/vars.sh && kubectl kuttl test --config ./test/integration/kuttl-engine-features.yaml

.PHONY: test-integration-operator-upgrade
test-integration-operator-upgrade: docker-build k3d-upload-image ## Run operator upgrade tests against K8S cluster
	. ./test/vars.sh && kubectl kuttl test --config ./test/integration/kuttl-operator-upgrade.yaml

.PHONY: test-e2e-core
test-e2e-core: docker-build k3d-upload-image ## Run e2e/core tests
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-core.yaml

.PHONY: test-e2e-db-upgrade
test-e2e-db-upgrade: docker-build k3d-upload-image ## Run e2e/db-upgrade tests
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-db-upgrade.yaml

.PHONY: test-e2e-operator-upgrade
test-e2e-operator-upgrade: docker-build k3d-upload-image ## Run e2e/operator-upgrade tests
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-operator-upgrade.yaml

.PHONY: test-e2e-data-importer
test-e2e-data-importer: docker-build k3d-upload-image ## Run e2e/data-importer tests
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-data-importer.yaml

.PHONY: test-e2e-data-importer-pg
test-e2e-data-importer-pg: docker-build k3d-upload-image ## Run e2e/data-importer PG test
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-data-importer.yaml --test pg

.PHONY: test-e2e-data-importer-psmdb
test-e2e-data-importer-psmdb: docker-build k3d-upload-image ## Run e2e/data-importer PSMDB test
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-data-importer.yaml --test psmdb

.PHONY: test-e2e-data-importer-pxc
test-e2e-data-importer-pxc: docker-build k3d-upload-image ## Run e2e/data-importer PXC test
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-data-importer.yaml --test pxc

.PHONY: test-e2e-engine-features
test-e2e-engine-features: docker-build k3d-upload-image ## Run e2e/engine-features tests
	. ./test/vars.sh && kubectl kuttl test --config ./test/e2e/kuttl-engine-features.yaml

.PHONY: k3d-cluster-up
k3d-cluster-up: ## Create a K8S cluster for testing.
	k3d cluster create --config ./test/k3d_config.yaml
	k3d kubeconfig get everest-operator-test > ./test/kubeconfig

.PHONY: k3d-cluster-up
k3d-cluster-down: ## Create a K8S cluster for testing.
	k3d cluster delete --config ./test/k3d_config.yaml
	rm -f ./test/kubeconfig || true

.PHONY: k3d-cluster-reset
k3d-cluster-reset: k3d-cluster-down k3d-cluster-up ## Recreate a K8S cluster for testing.

.PHONY: k3d-upload-image
k3d-upload-image:
	k3d image import -c everest-operator-test -m direct $(IMG)

# Cleanup all resources created by the tests
.PHONY: cluster-cleanup
cluster-cleanup:
	kubectl delete db --all-namespaces --all --cascade=foreground --ignore-not-found=true || true
	@namespaces=$$(kubectl get pxc -A -o jsonpath='{.items[*].metadata.namespace}'); \
	for ns in $$namespaces; do \
		kubectl -n $$ns get pxc -o name | xargs --no-run-if-empty -I{} kubectl patch -n $$ns {} -p '{"metadata":{"finalizers":null}}' --type=merge; \
	done
	@namespaces=$$(kubectl get psmdb -A -o jsonpath='{.items[*].metadata.namespace}'); \
	for ns in $$namespaces; do \
		kubectl -n $$ns get psmdb -o name | xargs --no-run-if-empty -I{} kubectl patch -n $$ns {} -p '{"metadata":{"finalizers":null}}' --type=merge; \
	done
	@namespaces=$$(kubectl get pg -A -o jsonpath='{.items[*].metadata.namespace}'); \
	for ns in $$namespaces; do \
		kubectl -n $$ns get pg -o name | xargs --no-run-if-empty -I{} kubectl patch -n $$ns {} -p '{"metadata":{"finalizers":null}}' --type=merge; \
	done
	@namespaces=$$(kubectl get db -A -o jsonpath='{.items[*].metadata.namespace}'); \
	for ns in $$namespaces; do \
		kubectl -n $$ns get db -o name | xargs --no-run-if-empty -I{} kubectl patch -n $$ns {} -p '{"metadata":{"finalizers":null}}' --type=merge; \
	done
	@namespaces=$$(kubectl get db -A -o jsonpath='{.items[*].metadata.namespace}'); \
	for ns in $$namespaces; do \
		kubectl -n $$ns delete -f ./test/testdata/minio --ignore-not-found || true; \
	done
	kubectl delete pvc --all-namespaces --all --ignore-not-found=true || true
	kubectl delete backupstorage --all-namespaces --all --ignore-not-found=true || true
	kubectl get ns -o name | grep kuttl | xargs --no-run-if-empty kubectl delete || true
	kubectl delete ns operators olm --ignore-not-found=true --wait=false || true
	sleep 10
	kubectl delete apiservice v1.packages.operators.coreos.com --ignore-not-found=true || true
	kubectl get crd -o name | grep .coreos.com$ | xargs --no-run-if-empty kubectl delete || true
	kubectl get crd -o name | grep .percona.com$ | xargs --no-run-if-empty kubectl delete || true
	kubectl delete crd postgresclusters.postgres-operator.crunchydata.com --ignore-not-found=true || true

##@ Docker build

# If you wish built the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64 ). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	docker build --build-arg FLAGS="$(FLAGS)" -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	docker push ${IMG}

# PLATFORMS defines the target platforms for  the manager image be build to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - able to use docker buildx . More info: https://docs.docker.com/build/buildx/
# - have enable BuildKit, More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image for your registry (i.e. if you do not inform a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To properly provided solutions that supports more than one platform you should use this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: test ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- docker buildx create --name project-v3-builder
	docker buildx use project-v3-builder
	- docker buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- docker buildx rm project-v3-builder
	rm Dockerfile.cross

.PHONY: bundle-build
bundle-build: ## Build the bundle image.
	docker build -f bundle.Dockerfile -t $(BUNDLE_IMG) .

.PHONY: bundle-push
bundle-push: ## Push the bundle image.
	$(MAKE) docker-push IMG=$(BUNDLE_IMG)

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | kubectl apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | kubectl delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | kubectl apply -f -

.PHONY: deploy-test
deploy-test: manifests kustomize ## Deploy controller adjusted for test to the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/test | kubectl apply -f -

.PHONY: undeploy
undeploy: ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | kubectl delete --ignore-not-found=$(ignore-not-found) -f -

##@ Build Dependencies

## Location to install dependencies to
LOCALBIN := $(CWD)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Versions
KUSTOMIZE_VERSION ?= v5.7.0
CONTROLLER_TOOLS_VERSION ?= v0.18.0
# Set the Operator SDK version to use.
OPERATOR_SDK_VERSION ?= v1.40.0
# Set the OPM version to use.
OPM_VERSION ?= v1.56.0
# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.30
# Kubebuilder version to download.
KUBEBUILDER_VERSION = v4.9.0

.PHONY: kustomize
KUSTOMIZE_INSTALL_SCRIPT = "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh"
KUSTOMIZE = $(LOCALBIN)/kustomize-$(KUSTOMIZE_VERSION)
kustomize: $(LOCALBIN) ## Download kustomize locally if necessary.
ifeq (,$(wildcard $(KUSTOMIZE)))
	curl -Ss $(KUSTOMIZE_INSTALL_SCRIPT) | bash -s -- $(subst v,,$(KUSTOMIZE_VERSION)) $(LOCALBIN)
	mv $(LOCALBIN)/kustomize $(KUSTOMIZE)
endif

.PHONY: controller-gen
CONTROLLER_GEN = $(LOCALBIN)/controller-gen-$(CONTROLLER_TOOLS_VERSION)
controller-gen: $(LOCALBIN) ## Download controller-gen locally if necessary.
ifeq (,$(wildcard $(CONTROLLER_GEN)))
	GOBIN=$(LOCALBIN) GOOS=$(OS) GOARCH=$(ARCH) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)
	mv $(LOCALBIN)/controller-gen $(CONTROLLER_GEN)
endif

.PHONY: operator-sdk
OPERATOR_SDK_DL_URL = https://github.com/operator-framework/operator-sdk/releases/download/$(OPERATOR_SDK_VERSION)
OPERATOR_SDK = $(LOCALBIN)/operator-sdk-$(OPERATOR_SDK_VERSION)
operator-sdk: $(LOCALBIN) ## Download operator-sdk locally if necessary.
ifeq (,$(wildcard $(OPERATOR_SDK)))
	@{ \
	set -e ;\
	curl -sSLo $(OPERATOR_SDK) ${OPERATOR_SDK_DL_URL}/operator-sdk_$(OS)_$(ARCH) ;\
	chmod +x $(OPERATOR_SDK) ;\
	}
endif

.PHONY: opm
OPM_DL_URL = https://github.com/operator-framework/operator-registry/releases/download/$(OPM_VERSION)
OPM = $(LOCALBIN)/opm-$(OPM_VERSION)
opm: $(LOCALBIN) ## Download opm locally if necessary.
ifeq (,$(wildcard $(OPM)))
	@{ \
	set -e ;\
	curl -sSLo $(OPM) $(OPM_DL_URL)/$(OS)-$(ARCH)-opm ;\
	chmod +x $(OPM) ;\
	}
endif

.PHONY: kubebuilder
KUBEBUILDER_DL_URL = https://github.com/kubernetes-sigs/kubebuilder/releases/download/$(KUBEBUILDER_VERSION)
KUBEBUILDER = $(LOCALBIN)/kubebuilder-$(KUBEBUILDER_VERSION)
kubebuilder: $(LOCALBIN) ## Download kubebuilder locally if necessary.
ifeq (,$(wildcard $(KUBEBUILDER)))
	@{ \
	set -e ;\
	curl -sSLo $(KUBEBUILDER) $(KUBEBUILDER_DL_URL)/kubebuilder_$(OS)_$(ARCH) ;\
	chmod +x $(KUBEBUILDER) ;\
	}
endif

##@ Release Preparation

.PHONY: bundle
bundle: manifests kustomize operator-sdk ## Generate bundle manifests and metadata, then validate generated files.
	$(OPERATOR_SDK) generate kustomize manifests -q
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	$(KUSTOMIZE) build config/manifests | $(OPERATOR_SDK) generate bundle $(BUNDLE_GEN_FLAGS)
	$(OPERATOR_SDK) bundle validate ./bundle

# A comma-separated list of bundle images (e.g. make catalog-build BUNDLE_IMGS=example.com/operator-bundle:v0.1.0,example.com/operator-bundle:v0.2.0).
# These images MUST exist in a registry and be pull-able.
BUNDLE_IMGS ?= $(BUNDLE_IMG)

# The image tag given to the resulting catalog image (e.g. make catalog-build CATALOG_IMG=example.com/operator-catalog:v0.2.0).
CATALOG_IMG ?= $(IMAGE_TAG_BASE)-catalog:v$(VERSION)

# Set CATALOG_BASE_IMG to an existing catalog image tag to add $BUNDLE_IMGS to that image.
ifneq ($(origin CATALOG_BASE_IMG), undefined)
FROM_INDEX_OPT := --from-index $(CATALOG_BASE_IMG)
endif

# Build a catalog image by adding bundle images to an empty catalog using the operator package manager tool, 'opm'.
# This recipe invokes 'opm' in 'semver' bundle add mode. For more information on add modes, see:
# https://github.com/operator-framework/community-operators/blob/7f1438c/docs/packaging-operator.md#updating-your-existing-operator
.PHONY: catalog-build
catalog-build: opm ## Build a catalog image.
	$(OPM) index add --container-tool docker --mode semver --tag $(CATALOG_IMG) --bundles $(BUNDLE_IMGS) $(FROM_INDEX_OPT)

# Push the catalog image.
.PHONY: catalog-push
catalog-push: ## Push a catalog image.
	$(MAKE) docker-push IMG=$(CATALOG_IMG)

## Location to output deployment manifests
DEPLOYDIR = ./deploy
$(DEPLOYDIR)/bundle.yaml: manifests kustomize ## Generate deploy/bundle.yaml
	$(KUSTOMIZE) build config/default -o $(DEPLOYDIR)/bundle.yaml

.PHONY: release
release:  generate manifests bundle format $(DEPLOYDIR)/bundle.yaml ## Prepare release
