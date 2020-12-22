.PHONY: all build push

BASE ?= ubuntu
TAG  ?= ${BASE}

alpine:
	$(eval BASE := alpine)

all: build push

build:
	docker build -f $(BASE).Dockerfile -t mastak/airflow_operator_stats:$(TAG) .

push:
	docker push mastak/airflow_operator_stats:$(TAG)

run:
	docker run --privileged --cap-add SYS_PTRACE -v /proc:/host-proc:ro \
	-e CUSTOM_PROCFS_PATH=/host-proc \
	mastak/airflow_operator_stats:$(TAG)

# --privileged: 使用该参数，container内的root拥有真正的root权限。否则，container内的root只是外部的一个普通用户权限。privileged启动的容器，可以看到很多host上的设备，并且可以执行mount。甚至允许你在docker容器中启动docker容器。
# -v: The -v flag mounts the directory into the container
# --cap-add: Add Linux capabilities
# -e: Set environment variables