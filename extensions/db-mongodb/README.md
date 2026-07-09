# MongoDB Database Extension

Provides MongoDB database connectivity for Universe's persistence layer.

## When to Use This

- You prefer document-oriented storage over relational tables
- You need flexible schema evolution for instance metadata
- You already run MongoDB in your infrastructure

## Configuration

Create `./database.json`:

```json
{
  "provider": "mongodb",
  "host": "localhost",
  "port": 27017,
  "database": "universe",
  "username": "universe",
  "password": "secret"
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `provider` | `"h2"` | Must be `"mongodb"` to activate this extension |
| `host` | `"localhost"` | MongoDB server host |
| `port` | `3306` | MongoDB port (change to `27017`) |
| `database` | `"universe"` | Database name |
| `username` | `"sa"` | Database user |
| `password` | `""` | Database password |

## Docker Compose Example

```yaml
services:
  universe:
    image: ghcr.io/universeclouddev/universe:latest
    volumes:
      - ./data:/data
      - ./database.json:/data/database.json:ro
    depends_on:
      - mongodb

  mongodb:
    image: mongo:7
    environment:
      MONGO_INITDB_DATABASE: universe
      MONGO_INITDB_ROOT_USERNAME: universe
      MONGO_INITDB_ROOT_PASSWORD: secret
    volumes:
      - mongo-data:/data/db

volumes:
  mongo-data:
```

## Authentication

If username and password are provided, the extension connects with SCRAM authentication. If both are empty, it connects without authentication (suitable for local development or authenticated networks).
