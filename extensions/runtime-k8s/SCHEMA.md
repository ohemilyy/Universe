# K8s Extension Configuration Schema

Configuration file: `./extensions/k8s/config.json`

## Top-Level Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `factoryName` | string | `"kube"` | Runtime key used in instance configs (e.g., `"runtime": "kube"`) |
| `namespace` | string | `"default"` | K8s namespace where Pods are created |
| `hostPortBindAddress` | string | `null` | Optional host IP address for `hostPort` publication. Use `"127.0.0.1"` for loopback-only publication; `null`, blank, and `"0.0.0.0"` omit `hostIP` for wildcard compatibility. |
| `image` | string | `"azul-zulu:25-jdk-alpine"` | Container image for instance Pods |
| `imagePullPolicy` | string | `"IfNotPresent"` | K8s image pull policy (`Always`, `IfNotPresent`, `Never`) |
| `workingDir` | string | `"/app"` | Working directory inside the Pod |
| `restartPolicy` | string | `"Never"` | K8s restart policy (`Always`, `OnFailure`, `Never`) |
| `serviceAccount` | string | `null` | K8s ServiceAccount name for the Pod |
| `nodeSelector` | object | `{}` | K8s node selector labels (`{"disktype": "ssd"}`) |
| `tolerations` | array | `[]` | K8s tolerations array (see below) |
| `env` | object | `{}` | Environment variables (`{"KEY": "value"}`) |
| `labels` | object | `{}` | Extra labels applied to Pods |
| `annotations` | object | `{}` | Extra annotations applied to Pods |
| `volumes` | array | `[]` | Additional K8s volumes (see below) |
| `volumeMounts` | array | `[]` | Additional volume mounts (see below) |
| `kubeConfigPath` | string | `null` | Path inside the container to the kubeconfig file |
| `masterUrl` | string | `null` | Optional: override the API server URL (rarely needed) |
| `timeoutSeconds` | int | `30` | How long to wait for a Pod to reach Running state |
| `hostDataPath` | string | `null` | Host path that maps to the container's data directory. See [SETUP.md](SETUP.md) for local mode, omit for cloud mode |
| `s3TemplateInit` | boolean | `true` | Enable automatic S3 template init containers in cloud mode. See [CLOUD.md](CLOUD.md) |
| `s3InitImage` | string | `"amazon/aws-cli:latest"` | Container image for S3 init containers |
| `s3Bucket` | string | `null` | Override S3 bucket for init containers |
| `s3Prefix` | string | `null` | Override S3 key prefix for init containers |

`Configuration.hostAddress` does not control Kubernetes host-port binding. Set
`hostPortBindAddress` in this extension configuration when a destination-scoped
host-port binding is required.

## Sub-Schemas

### `volumes` items

```json
{
  "name": "my-volume",
  "hostPath": "/host/path",      // Host path (local dev only)
  "emptyDir": true,              // Or: use an emptyDir volume
  "configMapName": "my-config",  // Or: mount a ConfigMap
  "secretName": "my-secret",     // Or: mount a Secret
  "claimName": "my-pvc"          // Or: mount a PVC
}
```

Exactly one of `hostPath`, `emptyDir`, `configMapName`, `secretName`, or `claimName` should be set per volume.

### `volumeMounts` items

```json
{
  "name": "my-volume",
  "mountPath": "/container/path",
  "readOnly": false
}
```

### `tolerations` items

```json
{
  "key": "dedicated",
  "operator": "Equal",
  "value": "universe",
  "effect": "NoSchedule"
}
```

## Full Example (Local Dev)

```json
{
  "factoryName": "kube",
  "namespace": "default",
  "image": "azul-zulu:25-jdk-alpine",
  "hostPortBindAddress": "127.0.0.1",
  "hostDataPath": "/opt/universe/data",
  "timeoutSeconds": 30,
  "env": {
    "JAVA_OPTS": "-XX:+UseG1GC"
  },
  "nodeSelector": {
    "disktype": "ssd"
  }
}
```

## Full Example (Cloud with S3 Templates)

```json
{
  "factoryName": "kube",
  "namespace": "default",
  "image": "azul-zulu:25-jdk-alpine",
  "timeoutSeconds": 30,
  "s3TemplateInit": true,
  "s3InitImage": "amazon/aws-cli:latest"
}
```

## Full Example (Cloud with Shared PVC)

```json
{
  "factoryName": "kube",
  "namespace": "default",
  "image": "azul-zulu:25-jdk-alpine",
  "workingDir": "/data/running",
  "timeoutSeconds": 30,
  "volumes": [
    { "name": "universe-data", "claimName": "universe-data" }
  ],
  "volumeMounts": [
    { "name": "universe-data", "mountPath": "/data" }
  ]
}
```
