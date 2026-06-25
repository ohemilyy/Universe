# Universe

A single-JAR orchestrator for deploying and managing application instances across a cluster of nodes. Originally inspired by Minecraft cloud systems (CloudNet, SimpleCloud), Universe is designed as a general-purpose instance orchestrator with a clean extension API.

## Features

- **Master/Wrapper Cluster** — One Master node exposes a REST API; any number of Wrapper nodes execute instances via Hazelcast task dispatch.
- **Template-Based Deployment** — Instances are created from templates (file trees) with dynamic variable replacement.
- **Pluggable Runtimes** — Built-in `screen` and `tmux` runtimes; Docker and Kubernetes support via extensions.
- **Remote Template Storage** — S3-backed template storage extension for centralized template management.
- **Mesh Networking** — Tailscale extension exposes mesh-network IPs as template variables for cross-node connectivity.
- **Console & REST Commands** — Full command system accessible via console or HTTP API.
- **Single Fat JAR** — Master and Wrapper run from the same JAR; node type is determined by configuration.
- **GitOps & ArgoCD** — Sync templates from Git; export Kubernetes manifests for ArgoCD tracking.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Master Node                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  Ktor REST  │  │   Hazelcast │  │   Console Commands  │ │
│  │    API      │  │   IMap/Exec │  │   (Cloud v2)        │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│         │                │                    │             │
│         ▼                ▼                    ▼             │
│  POST /api/instances  DeployInstanceTask   instance create │
│  PUT /api/instances   StopInstanceTask     instance stop   │
│  POST /api/commands   TemplateSyncTask     template sync   │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Hazelcast Cluster
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Wrapper Node(s)                       │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │ TaskRouter  │  │  Template   │  │  RuntimeProvider     │ │
│  │ (IExecutor) │  │   Manager   │  │ (screen/tmux/docker) │ │
│  └─────────────┘  └─────────────┘  └──────────────────────┘ │
│         │                │                    │             │
│         ▼                ▼                    ▼             │
│   Receive Tasks    Copy Templates      Start Processes      │
│   Route Actions    Replace Variables   Pipe Stdin           │
└─────────────────────────────────────────────────────────────┘
```

## Building

Requires **JDK 25+** and Gradle 9.5+.

```bash
./gradlew build
```

Artifacts are copied to `.built/` after build.

### Modules

| Module | Purpose |
|--------|---------|
| `api` | Shared data classes, task DTOs, runtime interfaces, command abstractions |
| `app` | Core orchestrator logic: Hazelcast, Ktor API, template management, commands |
| `loader` | Bootstrap classloader that downloads runtime deps and launches `app` |
| `extensions/extension-api` | Extension-facing interfaces (`Extension`, `TemplateStorageProvider`, etc.) |
| `extensions/runtime-docker` | Docker container runtime provider |
| `extensions/runtime-k8s` | Kubernetes Pod runtime provider |
| `extensions/tailscale` | Tailscale mesh-network IP template variables |
| `extensions/storage-s3` | AWS S3 template storage backend |
| `extensions/db-postgres` | PostgreSQL database provider |
| `extensions/db-mongodb` | MongoDB database provider |
| `extensions/db-redis` | Redis database provider |
| `extensions/metrics-prometheus` | Prometheus metrics export |
| `extensions/metrics-influxdb` | InfluxDB metrics export |
| `extensions/gitops` | Git-based template and config sync |
| `extensions/argocd` | ArgoCD manifest exporter |
| `extensions/discord` | Discord bot for cluster management |
| `extensions/example` | Reference extension implementation |
| `minecraft/minecraft-api` | JVM 8 compatible public API for Minecraft plugin developers |
| `minecraft/minecraft-modern` | Paper 1.21.11+ plugin with MiniMessage support |
| `minecraft/minecraft-legacy` | Spigot 1.8.8 plugin with legacy color codes |
| `minecraft/minecraft-velocity` | Velocity 3.5.0 proxy plugin |
| `minecraft/minecraft-bungee` | BungeeCord proxy plugin |
| `minecraft/minecraft-folia` | Folia 1.21+ plugin |

## Running

### Standalone JAR

The `loader` module produces a fat JAR with all dependencies embedded or downloaded at runtime.

```bash
java -jar .built/universe-loader-0.0.1.jar
```

On first run, the following files/directories are created:
- `./config.json` — Node identity and cluster configuration
- `./configuration/` — Instance configuration files (`.json`)
- `./templates/` — Template storage (`<group>/<name>/`)
- `./running/` — Active instance working directories
- `./extensions/` — Extension JARs and configs

### Configuration

**`./config.json`** (main node config):
```json
{
  "address": "127.0.0.1",
  "port": 6000,
  "apiPort": 7000,
  "nodeId": "node-1",
  "clusterName": "universe-cluster",
  "isMasterNode": true,
  "masterAddress": "127.0.0.1",
  "masterPort": 6000,
  "masterApiPort": 7000
}
```

**`./database.json`** (database config):
```json
{
  "provider": "h2",
  "url": "universe.db",
  "host": "localhost",
  "port": 3306,
  "database": "universe",
  "username": "sa",
  "password": ""
}
```

| Provider | Key | Notes |
|----------|-----|-------|
| H2 (embedded) | `h2` | Default, zero setup, file-based |
| MySQL | `mysql` | Built-in, requires running MySQL server |
| PostgreSQL | `postgres` | Via `extension-db-postgres` |
| MongoDB | `mongodb` | Via `extension-db-mongodb` |
| Redis | `redis` | Via `extension-db-redis` |

**`./configuration/default.json`** (instance config):
```json
{
  "name": "default",
  "runtime": "screen",
  "command": "java -jar server.jar",
  "static": false,
  "instanceGroups": [],
  "nodes": ["node-1"],
  "hostAddress": "127.0.0.1",
  "availablePorts": { "min": 25565, "max": 25570 },
  "minimumServiceCount": 1,
  "environmentVariables": {},
  "templateInstallationConfig": {
    "allOf": [{ "name": "base", "group": "server", "storage": "local", "priority": 0 }],
    "allInGroups": [],
    "oneOf": [],
    "oneInGroups": [],
    "onTemplatePasteOverridePresentFiles": false
  },
  "fileModifications": ["server.properties"],
  "properties": {}
}
```

### Docker Compose

Create a `docker-compose.yml`:

```yaml
services:
  universe-master:
    image: ghcr.io/universeclouddev/universe:latest
    container_name: universe-master
    stdin_open: true
    tty: true
    ports:
      - "7000:7000"   # REST API
      - "6000:6000"   # Hazelcast
    volumes:
      - ./data:/data

  # You can also add a wrapper, but the master should be able to run instances on its own
  universe-wrapper:
    image: ghcr.io/universeclouddev/universe:latest
    depends_on:
      - universe-master
    volumes:
      - ./wrapper-data:/data

```

**Important:** The Wrapper's `config.json` must set `isMasterNode: false` and point to the Master's Hazelcast address:
```json
{
  "isMasterNode": false,
  "masterAddress": "universe-master", // In this case, the Docker Compose service name, it could also be the Master's IP if running separately
  "masterPort": 6000,
  "masterApiPort": 7000
}
```

Run:
```bash
docker compose up -d
```

### Running Commands in Docker Compose

**Option 1: Interactive attach**
```bash
docker attach universe-master
# Then type commands directly
cluster status
instance list
stop
```

**Option 2: REST API (recommended)**
```bash
# Get all instances
curl http://localhost:7000/api/instances

# Create an instance
curl -X POST http://localhost:7000/api/instances \
  -H "Content-Type: application/json" \
  -d '{"configurationName": "default"}'

# Execute a console command via REST
curl -X POST http://localhost:7000/api/commands/execute \
  -H "Content-Type: application/json" \
  -d '{"command": "cluster status"}'
```

## Console Commands

All commands work both in the console and via `POST /api/commands/execute`.

| Command | Description |
|---------|-------------|
| `cluster status` | Show cluster members and local node info |
| `cluster nodes` | List all connected nodes |
| `instance list` | List all instances with state, host, port |
| `instance create <config>` | Create a new instance from a configuration |
| `instance stop <id>` | Stop an instance by ID |
| `instance info <id>` | Show full instance details |
| `instance execute <id> <cmd>` | Send a command to an instance's stdin |
| `config list` | List all loaded configurations |
| `config reload` | Reload configurations from `./configuration/` |
| `template list` | List local templates (group/name) |
| `template sync <pattern> <node>` | Sync template(s) to another cluster node |
| `extension list` | Show installed and loaded extensions |
| `extension reload` | Trigger `onReload()` on all extensions |
| `help` | Show all available commands |
| `stop` / `exit` | Graceful shutdown |

## REST API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/instances` | List all instances |
| `POST` | `/api/instances` | Create instance from configuration |
| `PUT` | `/api/instances/{id}/state` | Update instance state and heartbeat |
| `POST` | `/api/commands/execute` | Execute console command, return output |
| `DELETE` | `/api/instances/{id}` | Stop and remove an instance |
| `PATCH` | `/api/instances/{id}/lifecycle?target=start|stop|restart` | Lifecycle control |
| `POST` | `/api/instances/{id}/execute` | Send command to instance stdin |
| `GET` | `/api/instances/{id}/logs?lines=100` | Retrieve last N log lines |
| `WS` | `/api/instances/{id}/live-log` | WebSocket live log streaming |
| `GET` | `/api/ping` | Health check |
| `GET` | `/api/node` | Node info (version, resources, uptime) |
| `GET` | `/api/node/config` | Node configuration |
| `POST` | `/api/node/reload` | Reload node configuration |
| `WS` | `/api/console` | Interactive console WebSocket (master only) |
| `GET` | `/api/cluster/nodes` | List all cluster nodes |
| `GET` | `/api/cluster/nodes/{id}` | Node details with instances |
| `POST` | `/api/cluster/nodes/{id}/command` | Execute command on remote node |
| `GET` | `/api/configurations` | List all configurations |
| `GET` | `/api/configurations/{name}` | Get configuration by name |
| `PUT` | `/api/configurations/{name}` | Create or update configuration |
| `DELETE` | `/api/configurations/{name}` | Delete configuration |
| `GET` | `/api/templates` | List all templates |
| `GET` | `/api/templates/{group}/{name}` | Get template info |
| `POST` | `/api/templates/sync` | Sync templates matching pattern to all nodes |

## Logging

Universe uses two complementary logging systems:

| System | Purpose | Output |
|--------|---------|--------|
| **Console** (`gg.scala.universe.console`) | User-facing CLI output with styled arrows, badges, and tables | stdout (via JLine) |
| **SLF4J/Logback** | Framework internals (Hazelcast, Docker, K8s, Netty) | `./logs/universe.log` |

The `debug` flag in `./config.json` controls both:
- **Normal (`debug: false`)**: Console shows `INFO`+ operational messages; Logback console shows `WARN`+ only; full logs go to file
- **Debug (`debug: true`)**: Console shows `INFO`+ and `DEBUG` messages; Logback shows `INFO`+ for all loggers including frameworks

Log files are rotated daily and retained for **30 days**.

## Templates

Templates live in `./templates/<group>/<name>/`.

```
templates/
  global/
    server/
      server.properties
      plugins/
  lobby/
    default/
      server.properties
      world/
```

When an instance is created, `TemplateManager`:
1. Resolves templates from `TemplateInstallationConfig`
2. Copies them to `./running/<instance-id>/` in priority order
3. Scans files listed in `Configuration.fileModifications`
4. Replaces built-in variables:
   - `%PORT%` — allocated instance port
   - `%INSTANCE_ID%` — 6-character instance ID
   - `%MASTER_IP%` / `%MASTER_ADDRESS%` — master node address
   - `%MASTER_PORT%` — master Hazelcast port
   - `%MASTER_API_PORT%` — master REST API port
   - `%NODE_ID%` — local node ID
   - `%HOST_ADDRESS%` — local host address (or runtime-specific override)
   - `%CONFIGURATION_NAME%` — configuration name
5. Replaces extension-provided variables:
   - **K8s runtime**: `%NAMESPACE%`, `%SERVICE_DNS%`, `%POD_NAME%`
   - **Tailscale**: `%TAILSCALE_IP%`, `%TAILSCALE_MAGIC_DNS%`, `%TAILSCALE_HOSTNAME%`
6. Replaces custom variables from `Configuration.properties`:
   - Each entry `{ "myKey": "myValue" }` becomes `%myKey%` → `myValue`

**Template sync between nodes:**
```bash
template sync server/base node-2
template sync server/* node-2     # sync all in group
template sync * node-2            # sync all templates
```

## Extensions

Extensions are self-registering JARs placed in `./extensions/`.

**Runtime extensions:**
- `runtime-docker` — Docker container runtime
- `runtime-k8s` — Kubernetes Pod runtime
- `tailscale` — Mesh-network IP template variables

**Storage extensions:**
- `storage-s3` — AWS S3 template storage

**Database extensions:**
- `db-postgres` — PostgreSQL database provider
- `db-mongodb` — MongoDB database provider
- `db-redis` — Redis database provider

**Metrics extensions:**
- `metrics-prometheus` — Prometheus metrics export (`/api/metrics`)
- `metrics-influxdb` — InfluxDB metrics export

**DevOps extensions:**
- `gitops` — Sync templates and configs from Git
- `argocd` — Export Kubernetes manifests for ArgoCD

**Integration extensions:**
- `discord` — Discord bot for cluster management

**Extension structure:**
```kotlin
class MyExtension : Extension {
    override fun id() = "my-extension"
    override fun version() = "1.0.0"

    @Inject lateinit var registry: RuntimeRegistry

    override fun onLoad() {
        registry.register("my-runtime", MyRuntimeProvider())
    }
}
```

**S3 commands:**
```bash
s3 upload server/base      # upload template to S3
s3 download server/base    # download template from S3
```

## Minecraft Plugins

Universe provides first-class Minecraft integration through a standalone `:minecraft:api` module and three platform-specific plugins.

### `:minecraft:api` — Plugin Developer API

- **JVM 8 compatible**, zero external dependencies
- Provides `UniverseAPI` with `InstanceManager`, `ConfigurationManager`, and `TemplateManager`
- All async operations return `CompletableFuture<Optional<T>>` for Java ergonomics
- Not relocated in shadow JARs so dependent plugins can use API classes at runtime

```kotlin
// In your plugin's onEnable()
Universe.register(this)

// Access the API
val api = Universe.getAPI()
api.instanceManager.getInstances().thenAccept { instances ->
    // ...
}
```

### Modern Paper Plugin (`minecraft-modern`)

- Targets Paper 1.21.11+
- Uses **MiniMessage** for all chat formatting (`CC.kt` converts `&` codes internally)
- Commands: `/universe info`, `/universe players`, `/universe tps`
- Reports TPS, player count, and max players in heartbeat

### Legacy Spigot Plugin (`minecraft-legacy`)

- Targets Spigot 1.8.8
- Uses legacy `&` color codes via `ChatColor.translateAlternateColorCodes`
- Same `/universe` command set adapted for legacy Bukkit API

### Velocity Proxy Plugin (`minecraft-velocity`)

- Targets Velocity 3.5.0
- Uses MiniMessage via same `CC.kt` pattern as modern
- Polls instances from Master REST API and auto-registers them as Velocity servers
- Commands: `/universe info`, `/universe send <player> <server>`
- Auto-connect on player join with configurable strategies (`LEAST_POPULATED`, `MOST_POPULATED`, `RANDOM`)

### BungeeCord Proxy Plugin (`minecraft-bungee`)

- Targets BungeeCord (legacy proxy support)
- Uses legacy `&` color codes
- Same auto-connect and instance polling as Velocity

### Folia Plugin (`minecraft-folia`)

- Targets Folia 1.21+ (regionized tick scheduling)
- Same command set as modern Paper plugin
- Adapts to Folia's async entity API

### Plugin Master URL Configuration

The plugin resolves the Master REST API URL from (in priority order):

1. `universe.master.url` JVM system property
2. `UNIVERSE_MASTER_URL` environment variable
3. `master-url` in `plugins/Universe/config.yml`
4. Default `http://localhost:6000`

**Docker / Kubernetes networking:**

If the Minecraft server runs in a container or pod and the Master is on a different network, set the env var to a reachable address:

```bash
# Docker Compose → containerized server
-e UNIVERSE_MASTER_URL=http://host.docker.internal:6000

# Kubernetes pod → external Master
-e UNIVERSE_MASTER_URL=http://my-game-host.example.com:6000

# Kubernetes pod → in-cluster Master Service
-e UNIVERSE_MASTER_URL=http://universe-master-service:6000
```

### Building Minecraft Plugins

```bash
./gradlew :minecraft:minecraft-api:build
./gradlew :minecraft:minecraft-modern:build
./gradlew :minecraft:minecraft-legacy:build
./gradlew :minecraft:minecraft-velocity:build
```

Shadow JARs are output to `.built/`.

## Networking

### Port Allocation

Universe's `PortAllocator` checks three sources before assigning a port:

1. **Local in-memory allocations** — ports already assigned by this JVM instance
2. **Cluster-wide active instances** — queries Hazelcast for all `ONLINE`/`CREATING` instances and skips their ports
3. **OS-level availability** — attempts a `ServerSocket` bind + TCP connect probe to catch services already listening on the machine

This prevents port conflicts even when multiple configurations share overlapping ranges or when external services occupy ports.

### Cross-Node Connectivity

By default, instances advertise `hostAddress` from their configuration. For nodes on different networks, use:

- **Tailscale extension** — set `hostAddress: "%TAILSCALE_IP%"` for encrypted mesh-network connectivity
- **K8s headless Services** — in-cluster DNS: `universe-<id>.<namespace>.svc.cluster.local`
- **Public IP / NodePort** — configure `hostAddress` to the node's public IP and use K8s `NodePort` services

### Proxy Auto-Connect

The Velocity and BungeeCord plugins automatically connect players to backend instances on join. Strategies:

| Strategy | Behavior |
|----------|----------|
| `LEAST_POPULATED` | Send player to the instance with the fewest players |
| `MOST_POPULATED` | Send player to the instance with the most players (e.g. for minigame lobbies) |
| `RANDOM` | Pick a random instance |

Configure in `plugins/Universe/config.yml`:
```yaml
auto-connect: true
auto-connect-strategy: LEAST_POPULATED
```

### Adding a new runtime extension

1. Create `extensions/runtime-myprovider/`
2. Implement `RuntimeProvider` from `:api`
3. Implement `Extension` from `:extensions:extension-api`
4. Register provider in `onLoad()` via injected `RuntimeRegistry`
5. Add to `settings.gradle.kts`

### Adding a new storage extension

1. Create `extensions/storage-mybackend/`
2. Implement `TemplateStorageProvider` from `:extensions:extension-api`
3. Register in `onLoad()` via injected `TemplateStorageRegistry`

### Key rules
- Built-In extensions depend on `:api` and `:extensions:extension-api` by default
- Never depend on `:app` from an extension
- Use `runtimeDownload` for external dependencies
- Register via injected registries in `onLoad()`, not statically

## License

See [LICENSE](LICENSE) and [COPYRIGHT.txt](COPYRIGHT.txt).
