CONTROLLER_IMAGE ?= boanlab/multus-service-controller
AGENT_IMAGE      ?= boanlab/multus-service-agent
TAG              ?= dev
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
	sudo docker build --build-arg TARGET=controller -t $(CONTROLLER_IMAGE):$(TAG) .
	sudo docker build --build-arg TARGET=agent      -t $(AGENT_IMAGE):$(TAG) .

## import both images into every k3s node's containerd (no registry needed)
load: image
	@for img in $(CONTROLLER_IMAGE) $(AGENT_IMAGE); do \
	  tar=/tmp/$$(basename $$img).tar ; \
	  sudo docker save $$img:$(TAG) -o $$tar ; sudo chmod 644 $$tar ; \
	  echo "-> local" ; sudo k3s ctr images import $$tar >/dev/null ; \
	  for n in $(NODES); do \
	    echo "-> $$n" ; \
	    scp -q $$tar ubuntu@$$n:/tmp/ ; \
	    ssh ubuntu@$$n "sudo k3s ctr images import $$tar >/dev/null" ; \
	  done ; \
	done

deploy:
	kubectl apply -f deploy/rbac.yaml -f deploy/controller.yaml -f deploy/agent-daemonset.yaml

undeploy:
	kubectl delete -f deploy/agent-daemonset.yaml -f deploy/controller.yaml -f deploy/rbac.yaml --ignore-not-found

e2e: e2e-phase1 e2e-phase2

e2e-phase1:
	./test/e2e/phase1.sh

e2e-phase2:
	./test/e2e/phase2.sh

clean:
	rm -f /tmp/multus-service-*.tar
