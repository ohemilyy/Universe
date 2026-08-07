# Cloud Deployment Guide

Use this guide when Universe runs in a real cloud Kubernetes cluster (EKS, GKE, AKS) or when the K8s nodes are separate from Universe's host.

In cloud mode, `hostPath` doesn't make sense — the cluster nodes are separate VMs with their own filesystems. The pod gets an `emptyDir` volume for its working directory, which means **the pod starts with an empty working directory.**

You have three options for providing files to the pod:

1. **[Automatic S3 Template Init Containers](#automatic-s3-template-init-containers)** (recommended — zero config)
2. **[Shared PVC](#shared-pvc-approach)** (for when you need a persistent shared filesystem)
3. **[Alternative Approaches](#alternative-approaches)** (baked images, ConfigMaps)

---

## Automatic S3 Template Init Containers

This is the **recommended** and **least config** approach for cloud deployments.

### How it works

1. You store template zips in an S3 bucket (via the S3 storage extension)
2. When an instance is created, the K8s extension reads the instance's `templateInstallationConfig`
3. For each template with `storage: "s3"`, the extension **auto-generates an init container**
4. The init container downloads the template zip from S3 and extracts it into the pod's working directory
5. The main container starts only after all init containers complete

### Prerequisites

- S3-compatible bucket (AWS S3, MinIO, Wasabi, DigitalOcean Spaces, etc.)
- S3 storage extension enabled (`./extensions/s3/config.json` configured)
- K8s extension in cloud mode (`hostDataPath` omitted or `null`)

### Step 1: Configure S3

Create `./extensions/s3/config.json`:

```json
{
  "bucket": "my-universe-templates",
  "region": "us-east-1",
  "prefix": "templates/",
  "accessKey": "AKIA...",
  "secretKey": "..."
}
```

For non-AWS S3 (MinIO, Wasabi, etc.), also set `endpoint`:

```json
{
  "bucket": "universe-templates",
  "region": "us-east-1",
  "endpoint": "https://s3.wasabisys.com",
  "accessKey": "...",
  "secretKey": "..."
}
```

### Step 2: Upload templates to S3

Use the S3 extension's upload command, or manually place zips at:

```
s3://my-universe-templates/templates/<group>/<name>.zip
```

### Step 3: Configure K8s extension

Create `./extensions/k8s/config.json`. The only required fields are:

```json
{
  "factoryName": "kube",
  "namespace": "default",
  "image": "azul-zulu:25-jdk-alpine",
  "timeoutSeconds": 30
}
```

**Do NOT set `hostDataPath`.** The extension detects cloud mode automatically.

**Do NOT set `s3Bucket` or `s3Prefix`** unless you want to override the S3 extension's config.

### Step 4: Create an instance configuration with S3 templates

```json
{
  "name": "minecraft-lobby",
  "runtime": "kube",
  "command": "java -jar server.jar",
  "ramMB": 2048,
  "templateInstallationConfig": {
    "allOf": [
      { "name": "lobby", "group": "minecraft", "storage": "s3", "priority": 1 }
    ]
  }
}
```

That's it. When the instance starts, the K8s extension creates an init container that downloads `s3://my-universe-templates/templates/minecraft/lobby.zip` and extracts it before the main container runs.

### S3 Init Container Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `s3TemplateInit` | boolean | `true` | Enable automatic S3 template init containers in cloud mode |
| `s3InitImage` | string | `"amazon/aws-cli:latest"` | Container image for init containers. Must provide `aws` CLI and `unzip` |
| `s3Bucket` | string | `null` | Override S3 bucket. If null, reads from S3 extension config |
| `s3Prefix` | string | `null` | Override S3 key prefix. If null, reads from S3 extension config |

### Custom init image

If your cluster can't pull `amazon/aws-cli:latest` or you need a smaller image, build one with `aws` CLI + `unzip`:

```dockerfile
FROM alpine:latest
RUN apk add --no-cache aws-cli unzip
ENTRYPOINT ["sh"]
```

Then set in K8s config:

```json
{
  "s3InitImage": "my-registry/aws-cli-unzip:1.0"
}
```

---

## Shared PVC Approach

Use a shared PVC when you need a persistent shared filesystem that both Universe and the pods can access.

### Prerequisites

A Kubernetes StorageClass that supports `ReadWriteMany` (RWX):
- **AWS:** [EFS CSI Driver](https://docs.aws.amazon.com/eks/latest/userguide/efs-csi.html) → StorageClass `efs-sc`
- **GKE:** [Filestore CSI Driver](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/filestore-csi-driver) → StorageClass `standard-rwx`
- **Azure AKS:** [Azure Files CSI Driver](https://learn.microsoft.com/en-us/azure/aks/azure-files-csi) → StorageClass `azurefile-csi`
- **On-prem / Bare metal:** NFS-backed StorageClass

### Step 1: Create the PVC

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: universe-data
  namespace: default
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: efs-sc   # Change to your RWX StorageClass
  resources:
    requests:
      storage: 10Gi
```

### Step 2: Deploy Universe inside the cluster

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: universe
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: universe
  template:
    metadata:
      labels:
        app: universe
    spec:
      serviceAccountName: universe
      containers:
        - name: universe
          image: ghcr.io/universeclouddev/universe:latest
          ports:
            - containerPort: 6000
            - containerPort: 7000
          volumeMounts:
            - name: data
              mountPath: /data
          env:
            - name: KUBECONFIG
              value: ""
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: universe-data
---
apiVersion: v1
kind: Service
metadata:
  name: universe
  namespace: default
spec:
  selector:
    app: universe
  ports:
    - name: api
      port: 6000
      targetPort: 6000
    - name: cluster
      port: 7000
      targetPort: 7000
```

### RBAC

Universe needs permission to create/list/delete pods:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: universe-runtime
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/exec", "pods/log", "persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: universe-runtime
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: universe-runtime
subjects:
  - kind: ServiceAccount
    name: universe
    namespace: default
```

### Step 3: K8s extension config

```json
{
  "factoryName": "kube",
  "namespace": "default",
  "image": "azul-zulu:25-jdk-alpine",
  "workingDir": "/data/running",
  "volumes": [
    { "name": "universe-data", "claimName": "universe-data" }
  ],
  "volumeMounts": [
    { "name": "universe-data", "mountPath": "/data" }
  ],
  "timeoutSeconds": 30
}
```

**Note on volume mount detection:** If you provide a `volumeMount` whose path equals or contains the `workingDir`, the extension automatically skips creating the default `emptyDir` work volume. This prevents your PVC from being shadowed by an empty directory.

### Why `workingDir: "/data/running"`?

Universe creates instances at `./running/<instance-id>/`. With the PVC mounted at `/data`, the full path becomes `/data/running/<instance-id>/`. By setting `workingDir` to `/data/running`, the pod's working directory becomes `/data/running/<instance-id>/`, so commands like `java -jar server.jar` work without path adjustments.

---

## Alternative Approaches

If neither S3 init containers nor shared PVC work for your environment:

### Bake files into the container image

Simplest, but requires rebuilding the image for every template change:

```json
{
  "image": "your-registry/minecraft-server:1.21",
  "command": "java -jar server.jar"
}
```

### ConfigMap or Secret volumes

For static config files:

```json
{
  "volumes": [
    { "name": "server-config", "configMapName": "minecraft-config" },
    { "name": "server-secrets", "secretName": "minecraft-secrets" }
  ],
  "volumeMounts": [
    { "name": "server-config", "mountPath": "/app/config" },
    { "name": "server-secrets", "mountPath": "/app/secrets" }
  ]
}
```
