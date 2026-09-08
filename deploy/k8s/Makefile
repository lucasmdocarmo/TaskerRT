# Local cluster workflow. docker-desktop shares the Docker daemon, so images
# built here are visible to the cluster without a registry.
.PHONY: build apply delete status smoke logs

ROOT := $(shell git rev-parse --show-toplevel)

build:
	docker build -f $(ROOT)/deploy/Dockerfile --target taskerd -t tasker/taskerd:dev $(ROOT)
	docker build -f $(ROOT)/deploy/Dockerfile --target tasker-worker -t tasker/tasker-worker:dev $(ROOT)

apply:
	kubectl apply -f namespace.yaml -f taskerd.yaml -f worker.yaml
	kubectl -n tasker rollout status deploy/taskerd --timeout=120s
	kubectl apply -f scaledobject.yaml

delete:
	kubectl delete -f scaledobject.yaml -f worker.yaml -f taskerd.yaml --ignore-not-found
	kubectl delete namespace tasker --ignore-not-found

status:
	kubectl -n tasker get deploy,pods,scaledobject,hpa

logs:
	kubectl -n tasker logs deploy/taskerd --tail=50

smoke:
	./smoke.sh
