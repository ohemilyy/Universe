# K8s Extension Setup Guide

## Docker Compose Setup

If Universe runs inside Docker Compose (the recommended deployment), it does **not** have automatic access to a Kubernetes cluster like it would if running inside a Pod. You must explicitly provide a **kubeconfig file** and mount it into the container.

### Required docker-compose.yml changes

```yaml
universe:
  image: ghcr.io/universeclouddev/universe:latest
  ports:
    - "127.0.0.1:6000:6000"
    - "127.0.0.1:7000:7000"
  volumes:
    - ./data:/data
    - /var/run/docker.sock:/var/run/docker.sock
    # REQUIRED: Mount host kubeconfig into the container
    - ~/.kube/config:/root/.kube/config:ro
  environment:
    # Tell Fabric8 where to find kubeconfig
    - KUBECONFIG=/root/.kube/config
  # REQUIRED on Linux Docker: makes host.docker.internal resolve to the host
  extra_hosts:
    - "host.docker.internal:host-gateway"
  restart: unless-stopped
  tty: true
  stdin_open: true
```

**Why mount `~/.kube/config`?**

The Fabric8 Kubernetes client needs cluster credentials, CA certificates, and the API server URL. These are stored in your local `~/.kube/config` file. By mounting it into the container and setting `KUBECONFIG`, Universe can authenticate to the cluster just like `kubectl` does on your host.

**Why `extra_hosts: ["host.docker.internal:host-gateway"]`?**

Docker Desktop (Mac/Windows) automatically provides `host.docker.internal` as a DNS name that resolves to the host. On Linux Docker, this does **not** exist by default. The `extra_hosts` line adds it so that `host.docker.internal` resolves to the host gateway IP.

**No kubeconfig edits needed**

Universe automatically detects when it is running inside a Docker container. If your kubeconfig points to `127.0.0.1` or `localhost` (common for minikube, kind, Docker Desktop), Universe rewrites the API server URL to `host.docker.internal` on-the-fly so the container can reach the host's K8s API.

## Cluster-Specific Notes

### Docker Desktop Kubernetes
- Works out of the box with the compose config above
- Universe auto-rewrites `127.0.0.1:6443` to `host.docker.internal:6443`
- `host.docker.internal` is built-in on Docker Desktop

### minikube
- **Linux:** Start minikube, then start Universe with the compose config above. Universe handles the `127.0.0.1` → `host.docker.internal` rewrite automatically.
- **Mac/Windows (VM driver):** minikube runs in a VM. The API server is on the VM network, not the host. Use `network_mode: host` in docker-compose (Linux only) or switch to the Docker driver:
  ```bash
  minikube start --driver=docker
  ```
- If minikube uses a hostname like `minikube` in the kubeconfig, add it to `extra_hosts`:
  ```yaml
  extra_hosts:
    - "host.docker.internal:host-gateway"
    - "minikube:<minikube-ip>"
  ```

### Remote Clusters (EKS, GKE, AKS)
- Just mount `~/.kube/config` — the API URL is publicly accessible
- Ensure the Universe container has outbound internet access
- No additional changes needed

### kind / k3d
- These run K8s inside Docker containers on a private Docker network
- **Option 1 (Linux):** Use `network_mode: host` in docker-compose so the container shares the host network stack
- **Option 2 (all platforms):** Find the API server container IP and add it to `extra_hosts`:
  ```yaml
  extra_hosts:
    - "<api-server-hostname>:<container-ip>"
  ```

## Alternative: `network_mode: host` (Linux only)

If you prefer not to use `host.docker.internal`, you can make the Universe container share the host's network namespace:

```yaml
universe:
  # ... other config ...
  network_mode: host
```

This makes `127.0.0.1` inside the container actually mean the host's `127.0.0.1`, so no URL rewriting is needed. **Does not work on Docker Desktop (Mac/Windows).**
