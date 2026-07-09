# Contributing to Universe

Thank you for your interest in contributing to Universe. This document outlines the process and standards for contributing to the project.

## Table of Contents

- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Commit Message Format](#commit-message-format)
- [Pull Request Process](#pull-request-process)
- [Extension Development](#extension-development)
- [Architecture Rules](#architecture-rules)

## Getting Started

1. Ensure you have **JDK 25** installed and available on your `PATH`
2. Clone the repository
3. Run `./gradlew build` to verify the project compiles
4. Read `AGENTS.md` for detailed architectural guidelines

## Project Structure

```
├── api/                    # Shared interfaces, DTOs, schemas, Console system
├── app/                    # Core orchestrator: Hazelcast, Ktor, templates, commands
├── loader/                 # Bootstrap classloader with runtime dependency download
├── extensions/
│   ├── extension-api/      # Extension-facing interfaces
│   ├── runtime-docker/     # Docker runtime provider
│   ├── runtime-k8s/        # Kubernetes runtime provider
│   ├── storage-s3/         # S3 template storage
│   └── ...                 # Other extensions
└── minecraft/
    ├── minecraft-api/      # Public API for plugin developers (JVM 8, zero deps)
    ├── minecraft-modern/   # Paper 1.21.11+ plugin
    ├── minecraft-legacy/   # Spigot 1.8.8 plugin
    └── ...                 # Other platform plugins
```

## Development Setup

### Requirements

- **JDK 25** (required — the project targets `JvmTarget.JVM_25`)
- **Gradle** (wrapper included — use `./gradlew`)
- **Git** (for version control)

### Building

```bash
# Compile all modules
./gradlew compileKotlin

# Run tests
./gradlew test

# Build shadow JARs (output in .built/)
./gradlew shadowJar

# Full build
./gradlew build
```

### Running

The Universe orchestrator runs from a single fat JAR. Node type (Master vs Wrapper) is determined by `config.json`:

```json
{
  "isMasterNode": true
}
```

## Coding Standards

### Language & Framework

- **Kotlin 2.4.0** with `jvmTarget = 25`
- **Google Guice** for all dependency injection — no static singletons for DI
- **Gson** for all serialization — no other JSON libraries
- **Kotlin Coroutines** for file I/O and Ktor HTTP calls — never block Hazelcast event threads

### Dependencies

- All dependencies must be declared in `gradle/libs.versions.toml` (version catalog)
- Use `runtimeDownload` for runtime dependency resolution (handled by DependencyDownload)
- Never add direct Netty dependencies — Ktor Netty and Hazelcast handle networking

### Code Style

- Use **KDoc** for public API documentation
- Never use `/*` inside KDoc comments (use `/(*)` to avoid premature comment termination)
- Add `@author Luna` and `@date` to all new classes
- Only comment complicated code — avoid commenting obvious operations
- Follow Kotlin naming conventions and idiomatic patterns

### Console Output

- All user-facing CLI output must use the `Console` system in `:api` (`gg.scala.universe.console.Console`)
- Framework internals (Hazelcast, Docker, K8s) use SLF4J/Logback
- The `debug` flag in `config.json` controls both Console debug output and Logback levels

## Commit Message Format

Commit messages must be **lowercase** (unless emphasizing a class name) and prefixed with a type:

```
feat: PlayerProfileService will automatically fetch data from Mojang API
fix: everything should be 1.21.11
refractor: PlayerDataManager#getAllCurrentlyLoaded
chore: update gradle wrapper to 8.12
```

### YouTrack References

When implementing work related to a YouTrack ticket, the ticket ID **must** be referenced:

```
LUNA-1: add template variable replacement for port allocation
LUNA-3: implement RuntimeProvider interface for Docker extension
```

## Pull Request Process

1. **Create a branch** from `develop` (or `production` for hotfixes)
2. **Reference the YouTrack ticket** in your branch name and commits (e.g., `LUNA-42-feature-name`)
3. **Ensure the build passes**: `./gradlew build`
4. **Fill out the PR template** completely — all checklists must be addressed
5. **Request review** from the appropriate code owners (see `.github/CODEOWNERS`)

### PR Requirements

- All checklist items in the PR template must be completed
- Tests must pass on the CI build
- Extension isolation rules must be followed (if adding an extension)
- No breaking changes without explicit discussion and approval

## Extension Development

Extensions are self-registering JARs placed in `./extensions/`. They follow strict isolation rules.

### Rules

1. **No `:app` dependency** — extensions must never depend on the `:app` module
2. **Allowed dependencies** — only `:api` and `:extensions:extension-api`
3. **Self-registration** — extensions register themselves in their `onLoad()` method via Guice-injected registries
4. **Naming convention** — prefix by category: `runtime-*`, `storage-*`, `db-*`, `metrics-*`, etc.

### Creating an Extension

```kotlin
// In your extension's onLoad() method
class MyExtension @Inject constructor(
    private val runtimeRegistry: RuntimeRegistry
) : Extension {
    override fun onLoad() {
        runtimeRegistry.register("my-runtime", MyRuntimeProvider())
    }
}
```

### Testing Extensions

```bash
# Test a specific extension
./gradlew :extensions:runtime-docker:test

# Test all extensions
./gradlew :extensions:test
```

## Architecture Rules

### The Abstraction Rule

The core Wrapper daemon must **never** reference Docker directly. It only interacts with the `RuntimeProvider` interface via the `RuntimeRegistry`.

### Extension Isolation

Extensions must only depend on `:api` and `:extensions:extension-api`. No `:app` dependencies are allowed in any extension module.

### Zero Netty Leakage

No direct Netty dependencies should leak into the main fat JAR. Hazelcast and Ktor Netty handle all networking.

### Coroutine Safety

Use Kotlin Coroutines for:
- File reading/writing (template operations)
- Ktor HTTP calls
- Any I/O that could block

The main Hazelcast event threads must **never** be blocked.

### Instance IDs

Instance IDs are **6-character alphanumeric strings** generated from `UUID.randomUUID()`, not full UUIDs.

### Hazelcast Task Dispatch

Tasks sent over `IExecutorService` are serialized as **Gson-serialized JSON strings** (plain `String` payloads), not Hazelcast-native serialization.

---

## Questions?

- Check `AGENTS.md` for detailed agent personas and handoff protocols
- Check the YouTrack instance for active tickets and priorities
- Reach out on Discord for community support
