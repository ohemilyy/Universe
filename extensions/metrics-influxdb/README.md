# InfluxDB Metrics Extension

Exports Universe metrics to InfluxDB 2.x (or InfluxDB Cloud) for time-series analysis.

## When to Use This

- You use InfluxDB for long-term metric retention
- You want Flux queries and InfluxDB dashboards
- You need downsampling and retention policies

## Configuration

Create `./extensions/metrics-influxdb/config.json`:

```json
{
  "url": "http://localhost:8086",
  "token": "your-influxdb-token",
  "org": "universe",
  "bucket": "metrics",
  "intervalSeconds": 15
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `url` | `"http://localhost:8086"` | InfluxDB server URL |
| `token` | `""` | InfluxDB API token (required for 2.x) |
| `org` | `"universe"` | InfluxDB organization |
| `bucket` | `"metrics"` | InfluxDB bucket to write to |
| `intervalSeconds` | `15` | Metric flush interval |

## Docker Compose Example

```yaml
services:
  universe:
    image: ghcr.io/universeclouddev/universe:latest
    volumes:
      - ./data:/data

  influxdb:
    image: influxdb:2
    environment:
      DOCKER_INFLUXDB_INIT_MODE: setup
      DOCKER_INFLUXDB_INIT_USERNAME: admin
      DOCKER_INFLUXDB_INIT_PASSWORD: secret123
      DOCKER_INFLUXDB_INIT_ORG: universe
      DOCKER_INFLUXDB_INIT_BUCKET: metrics
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: your-influxdb-token
    volumes:
      - influxdb-data:/var/lib/influxdb2

volumes:
  influxdb-data:
```

## Metrics Format

Metrics are written as InfluxDB line protocol with the measurement `universe` and tags for node/instance IDs.

Example:
```
universe,node_id=node-1,config=lobby instances=5i,ram_mb=4096i 1716470400000000000
```

## Multiple Providers

Only one metrics provider can be active at a time. If both Prometheus and InfluxDB extensions are loaded, the last one registered wins. Use `extension list` to see which is active.
