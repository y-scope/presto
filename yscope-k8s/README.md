# Setup

## Dependency Installation

### Install docker

Follow the guide here: [docker]

### Install kubectl

`kubectl` is the command-line tool for interacting with Kubernetes clusters. You will use it to
manage and inspect your k3d cluster.

Follow the guide here: [kubectl]

### Install k3d

k3d is a lightweight wrapper to run k3s (Rancher Lab's minimal Kubernetes distribution) in docker.

Follow the guide here: [k3d]

### Runbook

Import the images and apply them to the cluster:
```shell
# Create a cluster called presto
# There should be two directories under /path/to/host/configs/:
#   - /path/to/host/configs/etc-coordinator:
#     - /path/to/host/configs/etc-coordinator/catalog/clp.properties
#     - /path/to/host/configs/etc-coordinator/config.properties
#     - /path/to/host/configs/etc-coordinator/jvm.config
#     - /path/to/host/configs/etc-coordinator/log.properties
#     - /path/to/host/configs/etc-coordinator/metadata-filter.json
#     - /path/to/host/configs/etc-coordinator/node.properties
#
#   - /path/to/host/configs/etc-worker:
#     - /path/to/host/configs/etc-worker/catalog/clp.properties
#     - /path/to/host/configs/etc-worker/config.properties
#     - /path/to/host/configs/etc-worker/node.properties
#     - /path/to/host/configs/etc-worker/velox.properties
#
# There will be examples of these config files in the following sections
k3d cluster create presto --servers 1 --agents 1 -v /path/to/host/configs/:/configs
# Load the coordinator image
k3d image import ghcr.io/y-scope/yscope-presto-with-clp-connector-coordinator:latest -c presto
# Load the worker image
k3d image import ghcr.io/y-scope/yscope-presto-with-clp-connector-worker:latest -c presto 
# Launch the container  
kubectl apply -f coordinator.yaml worker.yaml
```

To do a sanity check:
```shell
kubectl port-forward svc/presto-coordinator 8080:8080
# Check is coordinator alive
curl -X GET http://coordinator:8080/v1/info
# Check is worker connected to the coordinator
curl -X GET http://coordinator:8080/v1/nodes
```

# Example Configs for Coordinator

Example of k8s image YAML:
```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: coordinator
  name: coordinator
spec:
  containers:
    - name: coordinator
      image: ghcr.io/y-scope/yscope-0.293-coordinator:latest
      imagePullPolicy: Never
      volumeMounts:
        - name: "coordinator-config"
          mountPath: "/opt/presto-server/etc"
  volumes:
    - name: "coordinator-config"
      hostPath:
        path: "/configs/etc-coordinator"
---
apiVersion: v1
kind: Service
metadata:
  name: coordinator
  labels:
    app: coordinator
spec:
  type: NodePort
  ports:
    - port: 8080
      nodePort: 30000
      name: "8080"
  selector:
    app: coordinator
```

Example of `/path/to/host/configs/etc-coordinator/catalog/clp.properties` (need to update `clp.metadata-db-*` fields):
```
connector.name=clp
clp.metadata-provider-type=mysql
clp.metadata-db-url=jdbc:mysql://REPLACE_ME
clp.metadata-db-name=REPLACE_ME
clp.metadata-db-user=REPLACE_ME
clp.metadata-db-password=REPLACE_ME
clp.metadata-table-prefix=clp_
clp.split-provider-type=mysql
clp.metadata-filter-config=$(pwd)/etc-coordinator/metadata-filter.json
```

Example of `/path/to/host/configs/etc-coordinator/config.properties`:
```
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
query.max-memory=1GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://localhost:8080
optimizer.optimize-hash-generation=false
regex-library=RE2J
use-alternative-function-signatures=true
inline-sql-functions=false
nested-data-serialization-enabled=false
native-execution-enabled=true
```

Example of `/path/to/host/configs/etc-coordinator/jvm.config`:
```
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
```

Example of `/path/to/host/configs/etc-coordinator/log.properties`:
```
com.facebook.presto=DEBUG
```

Example of `/path/to/host/configs/etc-coordinator/metadata-filter.json`:
```
{
  "clp.default": [
    {
      "filterName": "msg.timestamp",
      "rangeMapping": {
        "lowerBound": "begin_timestamp",
        "upperBound": "end_timestamp"
      },
      "required": true
    }
  ]
}
```

Example of `/path/to/host/configs/etc-coordinator/node.properties`:
```
node.environment=production
node.id=coordinator
```

# Example Configs for Worker

Example of k8s image YAML:
🚧This is still in progress.
```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: worker
  name: worker
spec:
  containers:
    - name: worker
      image: ubuntu:22.04
      # imagePullPolicy: Never
      command:
        - /bin/bash
        - -c
      args:
        - sleep infinity
```

Example of `/path/to/host/configs/etc-worker/catalog/clp.properties`:
```
connector.name=clp
```

Example of `/path/to/host/configs/etc-worker/config.properties` (need to replace the `presto.version` to make it the same as coordinator`s):
```
discovery.uri=http://127.0.0.1:8080
presto.version=REPLACE_ME
http-server.http.port=7777
shutdown-onset-sec=1
register-test-functions=false
runtime-metrics-collection-enabled=false
```

Example of `/path/to/host/configs/etc-worker/node.properties`:
```
node.environment=production
node.internal-address=127.0.0.1
node.location=testing-location
node.id=worker-1
```

Example of `/path/to/host/configs/etc-worker/velox.properties`:
```
mutable-config=true
```

[docker]: https://docs.docker.com/engine/install
[k3d]: https://k3d.io/stable/#installation
[kubectl]: https://kubernetes.io/docs/tasks/tools/#kubectl