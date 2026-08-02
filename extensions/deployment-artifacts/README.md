# Artifact Deployment Extension

The Artifact Deployment Extension keeps local server and plugin files in sync with artifacts published through Forgejo, JFrog Artifactory, and TeamCity.

It periodically checks each configured source for changes. When a newer or different artifact is found, the extension downloads it and atomically replaces the existing local file. That means the old artifact is not removed until the replacement is ready, which helps avoid partially written or corrupted files.

## Configuration

On its first startup, the extension creates the following configuration file:

```text
./extensions/deployment-artifacts/config.json
```

All values provided through `targetPath` must be relative to the Universe working directory. Absolute paths are not supported.

```json
{
  "enabled": true,
  "pollIntervalSeconds": 60,
  "requestTimeoutSeconds": 30,
  "forgejo": [
    {
      "id": "lobby-plugin",
      "enabled": true,
      "serverUrl": "https://code.example.com",
      "owner": "minecraft",
      "repository": "lobby-plugin",
      "assetPattern": "lobby-*.jar",
      "targetPath": "templates/lobby/default/plugins/Lobby.jar",
      "branch": "production",
      "releaseTag": null,
      "includePrereleases": false,
      "token": null
    }
  ],
  "artifactory": [
    {
      "id": "paper-server",
      "enabled": true,
      "serverUrl": "https://company.jfrog.io",
      "repository": "minecraft-releases",
      "artifactPath": "servers/{branch}/server.jar",
      "targetPath": "templates/server/default/server.jar",
      "branch": "production",
      "accessToken": null,
      "username": null,
      "password": null
    }
  ],
  "teamCity": [
    {
      "id": "proxy-plugin",
      "enabled": true,
      "serverUrl": "https://teamcity.example.com",
      "buildTypeId": "Minecraft_ProxyPlugin",
      "artifactPath": "build/libs/ProxyPlugin.jar",
      "targetPath": "templates/proxy/default/plugins/ProxyPlugin.jar",
      "branch": "production",
      "accessToken": null,
      "username": null,
      "password": null
    }
  ]
}
```

The extension can be disabled globally with the top-level `enabled` option. Individual sources may also be turned off without deleting their configuration by setting their own `enabled` value to `false`.

`pollIntervalSeconds` controls how frequently remote sources are checked, while `requestTimeoutSeconds` defines how long an individual network request may run before it is cancelled.

## Authentication

Authentication depends on the artifact provider being used.

### Forgejo

Public Forgejo repositories do not require authentication. For private repositories, provide a Forgejo access token with permission to read the repository and its releases.

```json
"token": "your-forgejo-token"
```

### JFrog Artifactory

Artifactory supports either an access token or a username and password.

When both methods are configured, `accessToken` takes precedence. The username and password are only used when no access token has been provided.

### TeamCity

TeamCity may be accessed with either a bearer access token or basic username-and-password authentication.

As with Artifactory, an access token takes priority whenever one is present.

Authentication values are read only from the extension’s local configuration file. Tokens, usernames, and passwords are never included in extension logs.

## Branch and release selection

Each provider handles branches slightly differently.

### Forgejo

Forgejo release entries are filtered using their `target_commitish` value.

Set `branch` to a branch such as `production` when the extension should only accept releases associated with that branch. Leave it empty to accept the newest published release regardless of branch.

To select one specific release instead, provide its exact tag through `releaseTag`:

```json
"releaseTag": "v1.4.2"
```

When `releaseTag` is set, the extension looks for that tag rather than simply choosing the newest available release.

The `assetPattern` field determines which release asset should be downloaded. Wildcards may be used, as shown with `lobby-*.jar`.

Prerelease builds are ignored by default. Set `includePrereleases` to `true` when they should also be considered.

### JFrog Artifactory

For Artifactory sources, every `{branch}` placeholder inside `artifactPath` is replaced with the configured branch name.

For example:

```text
servers/{branch}/server.jar
```

With `"branch": "production"`, this becomes:

```text
servers/production/server.jar
```

### TeamCity

TeamCity retrieves the newest successful build matching the configured build type and branch.

Leave `branch` empty when the build configuration’s default branch should be used.

## Updating artifacts

Artifacts are replaced atomically after their content changes. This avoids leaving behind a half-downloaded JAR if a request fails, the connection drops, or the process is interrupted at an awkward moment.

Already-running Universe instances are not modified or restarted. The updated artifact only affects files created or copied after the replacement occurs.

For that reason, deployment targets should normally be placed beneath `templates/`. Newly created instances will then receive the updated server or plugin artifact automatically.

For example:

```text
templates/lobby/default/plugins/Lobby.jar
templates/server/default/server.jar
templates/proxy/default/plugins/ProxyPlugin.jar
```
