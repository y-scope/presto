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

3. Replace the content of `/path/to/clp-json-package/etc/clp-config.yml` with the output of `demo-assets/init.sh <ip_addr>` where the `<ip_addr>` is the IP address of the host that you are running the clp-package.

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

6. Use the following command to update the CLP metadata database so that the worker can find the archives in right place:
```bash
# Install mysql-client if necessary
sudo apt update && sudo apt install -y mysql-client
# Find the user and password in /path/to/clp-json-package/etc/credential.yml
mysql -h ${REPLACE_IP} -P 6001 -u ${REPLACE_USER} -p'${REPLACE_PASSWORD}' clp-db -e "UPDATE clp_datasets SET archive_storage_directory = '/var/data/archives/default';"
```

# Create k8s Cluster
Create a local k8s cluster with port forwarding
```bash
k3d cluster create yscope --servers 1 --agents 1 -v $(readlink -f /path/to/clp-json-package/var/data/archives):/var/data/archives
```

# Working with helm chart
## Install
In `yscope-k8s/templates/presto/presto-coordinator-config.yaml`:
1. replace the `${REPLACE_ME}` in `clp.metadata-db-url=jdbc:mysql://${REPLACE_ME}:6001` with the IP address of the host you are running the clp-package (basially match the IP address that you configured in the `etc/clp-config.yml` of the clp-package).
2. replace the `${REPLACE_ME}` in `clp.metadata-db-user=${REPLACE_ME}` with the user stored in `/path/to/clp-json-package/etc/credential.yml`.
3. replace the `${REPLACE_ME}` in `clp.metadata-db-password=${REPLACE_ME}` with the password stored in `/path/to/clp-json-package/etc/credential.yml`.

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
