# Setup local K8s cluster for presto + clp

## Install docker

Follow the guide here: [docker]

## Install kubectl

`kubectl` is the command-line tool for interacting with Kubernetes clusters. You will use it to
manage and inspect your k3d cluster.

Follow the guide here: [kubectl]

## Install k3d

k3d is a lightweight wrapper to run k3s (Rancher Lab's minimal Kubernetes distribution) in docker.

Follow the guide here: [k3d]

## Install Helm

Helm is the package manager for Kubernetes.

Follow the guide here: [helm]

# Launch clp-package
1. Find the clp-package for test on our official website [clp-json-v0.4.0]. We also put the dataset for demo here: `mongod-256MB-presto-clp.log.tar.gz`.

2. Untar it.

3. Replace the content of `etc/clp-config.yml` with the following (also replace the IP address `${REPLACE_IP}` with the actual IP address of the host that you are running the clp-package):
```yaml
package:
  storage_engine: "clp-s"
database:
  type: "mariadb"
  host: "${REPLACE_IP}"
  port: 6001
  name: "clp-db"
query_scheduler:
  host: "${REPLACE_IP}"
  port: 6002
  jobs_poll_delay: 0.1
  num_archives_to_search_per_sub_job: 16
  logging_level: "INFO"
queue:
  host: "${REPLACE_IP}"
  port: 6003
redis:
  host: "${REPLACE_IP}"
  port: 6004
  query_backend_database: 0
  compression_backend_database: 1
reducer:
  host: "${REPLACE_IP}"
  base_port: 6100
  logging_level: "INFO"
  upsert_interval: 100
results_cache:
  host: "${REPLACE_IP}"
  port: 6005
  db_name: "clp-query-results"
  stream_collection_name: "stream-files"
webui:
  host: "localhost"
  port: 6000
  logging_level: "INFO"
log_viewer_webui:
  host: "localhost"
  port: 6006
```

4. Launch:
```bash
# You probably want to run in a 3.11 python environment
sbin/start-clp.sh
```

5. Compress:
```bash
# You can also use your own dataset
sbin/compress.sh --timestamp-key 't.dollar_sign_date' datasets/mongod-256MB-processed.log
```

6. Use a JetBrain IDE to connect the database source. The database is `clp-db`, the user is `clp-user` and the password is in `etc/credential.yml`. Then modify the `archive_storage_directory` field in `clp_datasets` table to `/var/data/archives/default`, and submit the change.

# Create k8s Cluster
Create a local k8s cluster with port forwarding
```bash
# Replace the ~/clp-json-x86_64-v0.4.0/var/data/archives to the correct path
k3d cluster create yscope --servers 1 --agents 1 -v $(readlink -f ~/clp-json-x86_64-v0.4.0/var/data/archives):/var/data/archives
```

# Working with helm chart
## Install
In `yscope-k8s/templates/presto/presto-coordinator-config.yaml` replace the `${REPLACE_IP}` in `clp.metadata-db-url=jdbc:mysql://${REPLACE_IP}:6001` with the IP address of the host you are running the clp-package (basially match the IP address that you configured in the `etc/clp-config.yml` of the clp-package).

```bash
cd yscope-k8s

helm template . 

helm install demo .
```

## Use cli:
After all containers are in "Running" states (check by `kubectl get pods`):
```bash
kubectl port-forward service/presto-coordinator 8080:8080
```

Then you can further forward the 8080 port to your local laptop, to access the Presto's WebUI by e.g., http://localhost:8080

To use presto-cli:
```bash
./presto-cli-0.293-executable.jar --catalog clp --schema default --server localhost:8080
```

Example query:
```
SELECT * FROM default LIMIT 1;
```

## Uninstall
```bash
helm uninstall demo
```

# Delete k8s Cluster
```bash
k3d cluster delete yscope
```


[clp-json-v0.4.0]: https://github.com/y-scope/clp/releases/tag/v0.4.0
[docker]: https://docs.docker.com/engine/install
[k3d]: https://k3d.io/stable/#installation
[kubectl]: https://kubernetes.io/docs/tasks/tools/#kubectl
[helm]: https://helm.sh/docs/intro/install/
