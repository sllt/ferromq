IMAGE ?= sllt/ferromq
VERSION ?= $(shell awk '/^\[workspace.package\]/{in_pkg=1; next} /^\[/{in_pkg=0} in_pkg && /^version = /{gsub(/"/, "", $$3); print $$3; exit}' Cargo.toml)

all: release

release:
	cargo build -p ferromqd --release

# Docker image target: Docker Hub sllt/ferromq.

release-amd64:
	cargo build -p ferromqd --release --target x86_64-unknown-linux-musl

docker-amd64:
	docker build --no-cache -t $(IMAGE):$(VERSION)-amd64 -f Dockerfile.amd64 ./
	docker build --no-cache -t $(IMAGE):latest-amd64 -f Dockerfile.amd64 ./

publish-amd64:
	docker push $(IMAGE):$(VERSION)-amd64
	docker push $(IMAGE):latest-amd64

release-aarch64:
	cargo build -p ferromqd --release --target aarch64-unknown-linux-musl

docker-aarch64:
	docker build --no-cache -t $(IMAGE):$(VERSION)-arm64 -f Dockerfile.aarch64 ./
	docker build --no-cache -t $(IMAGE):latest-arm64 -f Dockerfile.aarch64 ./

publish-aarch64:
	docker push $(IMAGE):$(VERSION)-arm64
	docker push $(IMAGE):latest-arm64

merge:
	docker buildx imagetools create --tag $(IMAGE):$(VERSION) $(IMAGE):$(VERSION)-amd64 $(IMAGE):$(VERSION)-arm64
	docker buildx imagetools create --tag $(IMAGE):latest $(IMAGE):latest-amd64 $(IMAGE):latest-arm64

clean:
	cargo clean
