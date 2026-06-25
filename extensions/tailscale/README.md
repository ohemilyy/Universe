# Tailscale Extension

Exposes your node's **Tailscale mesh-network IP** as a template variable, so instances can advertise a stable, routable address across your tailnet.

## When to Use This

- You run Universe nodes on different machines / VPSs / home servers
- You want instances to be reachable via Tailscale's encrypted mesh network
- You don't want to deal with public IPs, port forwarding, or K8s Services

## How It Works

The extension shells out to the `tailscale` CLI and parses `tailscale status --json` to extract:

- `%TAILSCALE_IP%` — IPv4 address (e.g. `100.64.1.1`)
- `%TAILSCALE_IP6%` — IPv6 address
- `%TAILSCALE_HOSTNAME%` — machine hostname in Tailscale
- `%TAILSCALE_MAGIC_DNS%` — full MagicDNS name (e.g. `myhost.tailxxxxx.ts.net`)
- `%TAILSCALE_ADDRESS%` — alias for `%TAILSCALE_IP%`

These variables are available during template installation, so you can set:

```json
{
  "name": "lobby",
  "hostAddress": "%TAILSCALE_IP%",
  "availablePorts": { "min": 25565, "max": 25570 }
}
```

Then the proxy (Velocity / Bungee) connects directly to `100.64.1.1:25565` over the Tailscale tunnel.

## Docker Compose Setup

The Tailscale CLI is a thin client — it needs to talk to the `tailscaled` daemon via a UNIX socket. Mount **both** the binary and the socket directory:

```yaml
services:
  universe:
    image: ghcr.io/universeclouddev/universe:latest
    volumes:
      - ./data:/data
      - /usr/bin/tailscale:/usr/bin/tailscale:ro       # binary
      - /var/run/tailscale:/var/run/tailscale          # daemon socket
    # ... rest of your config
```

Verify inside the container:

```bash
docker exec <container> tailscale status
```

## Configuration

Create `./extensions/tailscale/config.json`:

```json
{
  "binaryPath": "tailscale",
  "timeoutMs": 5000,
  "warnIfUnavailable": true,
  "socketPath": "/var/run/tailscale/tailscaled.sock"
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `binaryPath` | `"tailscale"` | Path to the `tailscale` binary. Change if it's not in `PATH`. |
| `timeoutMs` | `5000` | Max wait time for `tailscale status --json` to respond. |
| `warnIfUnavailable` | `true` | Log a warning on startup if Tailscale is not running. |
| `socketPath` | `null` | Path to the `tailscaled` daemon socket. Required in Docker when the socket is mounted at a non-standard location. Common values: `/var/run/tailscale/tailscaled.sock`, `/run/tailscale/tailscaled.sock`. |

## Using the Variables

### In `hostAddress`

```json
{
  "name": "lobby",
  "hostAddress": "%TAILSCALE_IP%",
  "runtime": "screen"
}
```

### In `server.properties` (via `fileModifications`)

```json
{
  "fileModifications": ["server.properties"],
  "properties": {
    "server-ip": "%TAILSCALE_IP%"
  }
}
```

### In environment variables

```json
{
  "environmentVariables": {
    "UNIVERSE_HOST": "%TAILSCALE_IP%",
    "UNIVERSE_MAGIC_DNS": "%TAILSCALE_MAGIC_DNS%"
  }
}
```

## Fallback Behavior

If Tailscale is not running or the binary is missing:
- Variables resolve to empty strings
- The configuration's `hostAddress` falls back to its literal value
- The extension logs a warning (if `warnIfUnavailable: true`)

## Troubleshooting

**"No Tailscale IP detected" warning**

```bash
# Check tailscaled is running on the host
sudo systemctl status tailscaled

# Verify the socket is accessible inside the container
docker exec <container> ls -la /var/run/tailscale/

# Test the CLI manually
docker exec <container> tailscale status
```

**Permission denied on socket**

If `tailscaled` runs as non-root (e.g. systemd user service), the container user may not have access. Either:
- Run `tailscaled` as root, or
- Set the socket permissions to allow group read (`chmod g+r /var/run/tailscale/tailscaled.sock`)

## Architecture

- `TailscaleClient` — spawns `tailscale status --json`, parses with Gson, 30s in-memory cache
- `TailscaleVariableProvider` — implements `TemplateVariableProvider`, queried during instance deploy
- `TailscaleExtension` — Guice-managed, registers provider on `onLoad()`

Zero external dependencies beyond `:api` and `:extensions:extension-api`.
