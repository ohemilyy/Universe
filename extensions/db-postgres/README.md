# PostgreSQL Database Extension

Provides PostgreSQL database connectivity for Universe's persistence layer.

## When to Use This

- You need a production-grade relational database
- You want concurrent access from multiple Universe nodes
- You need advanced querying via jOOQ with PostgreSQL-specific features

## Configuration

Create `./database.json`:

```json
{
  "provider": "postgres",
  "host": "localhost",
  "port": 5432,
  "database": "universe",
  "username": "universe",
  "password": "secret"
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `provider` | `"h2"` | Must be `"postgres"` to activate this extension |
| `host` | `"localhost"` | PostgreSQL server host |
| `port` | `3306` | PostgreSQL port (change to `5432`) |
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
      - postgres

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: universe
      POSTGRES_USER: universe
      POSTGRES_PASSWORD: secret
    volumes:
      - postgres-data:/var/lib/postgresql/data

volumes:
  postgres-data:
```

## Built-in Providers

Universe includes built-in H2 and MySQL providers. Extensions add:

| Provider | Extension | Key |
|----------|-----------|-----|
| H2 | Built-in | `h2` |
| MySQL | Built-in | `mysql` |
| PostgreSQL | `extension-db-postgres` | `postgres` |
| MongoDB | `extension-db-mongodb` | `mongodb` |
| Redis | `extension-db-redis` | `redis` |

## jOOQ Integration

All SQL operations use jOOQ Kotlin extensions. The extension sets up a `DSLContext` configured for PostgreSQL dialect.
