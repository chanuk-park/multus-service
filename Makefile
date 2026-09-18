IMAGE ?= boanlab/multus-service-controller
TAG   ?= dev
KUBECONFIG ?= /etc/rancher/k3s/k3s.yaml
export KUBECONFIG

.PHONY: build test vet fmt run image load deploy undeploy e2e clean

build:
	go build ./...

test:
	go test ./... -count=1

vet:
	go vet ./...

fmt:
	gofmt -l -w .

## run the controller out of cluster against the current kubeconfig
run:
	go run ./cmd/controller --events-file=- --zap-log-level=info

image:
	sudo docker build --build-arg TARGET=controller -t $(IMAGE):$(TAG) .

## import the image into every k3s node's containerd (no registry needed)
load: image
	sudo docker save $(IMAGE):$(TAG) -o /tmp/$(notdir $(IMAGE)).tar
	sudo k3s ctr images import /tmp/$(notdir $(IMAGE)).tar
	@for n in $(NODES); do \
	  echo "-> $$n"; \
	  scp -q /tmp/$(notdir $(IMAGE)).tar ubuntu@$$n:/tmp/ ; \
	  ssh ubuntu@$$n "sudo k3s ctr images import /tmp/$(notdir $(IMAGE)).tar" ; \
	done

deploy:
	kubectl apply -f deploy/rbac.yaml -f deploy/controller.yaml

undeploy:
	kubectl delete -f deploy/controller.yaml -f deploy/rbac.yaml --ignore-not-found

e2e:
	./test/e2e/phase1.sh

clean:
	rm -f /tmp/multus-service-controller.tar
