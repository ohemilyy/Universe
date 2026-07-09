# Redis Database Extension

Provides Redis database connectivity for Universe's persistence layer.

## When to Use This

- You need fast, in-memory storage for ephemeral data
- You want to use Redis as a caching layer or message broker
- You run a Redis cluster for high availability

## Configuration

Create `./database.json`:

```json
{
  "provider": "redis",
  "host": "localhost",
  "port": 6379,
  "username": "",
  "password": "secret"
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `provider` | `"h2"` | Must be `"redis"` to activate this extension |
| `host` | `"localhost"` | Redis server host |
| `port` | `3306` | Redis port (change to `6379`) |
| `username` | `""` | Redis username (ACL, Redis 6+) |
| `password` | `""` | Redis password |

## Notes

- The `database` field in `database.json` is ignored by the Redis provider
- The extension uses Lettuce as the Redis client for reactive/non-blocking operations
- Supports Redis Sentinel and Cluster modes via URL configuration (advanced)

## Docker Compose Example

```yaml
services:
  universe:
    image: ghcr.io/universeclouddev/universe:latest
    volumes:
      - ./data:/data
      - ./database.json:/data/database.json:ro
    depends_on:
      - redis

  redis:
    image: redis:7-alpine
    volumes:
      - redis-data:/data

volumes:
  redis-data:
```
