# Staging Provider

Defines the *staging* vocabulary shared by [`keboola/input-mapping`](../input-mapping) and
[`keboola/output-mapping`](../output-mapping), and wraps the lifecycle of Keboola Connection workspaces.

It answers two questions:

* **Where does data live during a job?** — `Staging\StagingType` and `Staging\StagingProvider`
* **How do I get a workspace to put it in?** — `Workspace\WorkspaceProvider`

The library itself does no reading or writing; the mapping libraries build their strategies from what it
exposes.

## Installation

```bash
composer require keboola/staging-provider
```

## Staging types

`Staging\StagingType` is the enum both mapping libraries branch on:

| `StagingType`         | value                  | `StagingClass` |
|-----------------------|------------------------|----------------|
| `Local`               | `local`                | `Disk`         |
| `S3`                  | `s3`                   | `Disk`         |
| `Abs`                 | `abs`                  | `Disk`         |
| `WorkspaceSnowflake`  | `workspace-snowflake`  | `Workspace`    |
| `WorkspaceBigquery`   | `workspace-bigquery`   | `Workspace`    |
| `None`                | `none`                 | `None`         |

`StagingType::getStagingClass()` collapses the type into the coarse distinction the rest of the code cares
about — data on disk versus data in a database workspace.

## `StagingProvider`

`Staging\StagingProvider` resolves a staging type plus a local path plus an optional workspace id into the
four staging slots a mapping run needs:

```php
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;

$stagingProvider = new StagingProvider(
    StagingType::WorkspaceSnowflake,
    '/data',            // local staging path
    '1234',             // staging workspace id, or null for disk staging
);

$stagingProvider->getStagingType();          // StagingType::WorkspaceSnowflake
$stagingProvider->getTableDataStaging();     // WorkspaceStaging('1234')
$stagingProvider->getTableMetadataStaging(); // LocalStaging('/data')
$stagingProvider->getFileDataStaging();      // LocalStaging('/data')
$stagingProvider->getFileMetadataStaging();  // LocalStaging('/data')
```

Only **table data** ever lives in a workspace. Files, file metadata and table metadata (manifests) are
always local, whatever the staging type — which is why the provider exposes four separate getters rather
than one staging object.

The constructor enforces that a workspace id is passed exactly when the staging class is `Workspace`, and
throws `InvalidArgumentException` otherwise.

Both staging kinds implement `Staging\StagingInterface`:

* `Staging\File\LocalStaging` (`FileStagingInterface`) — exposes `getPath()`
* `Staging\Workspace\WorkspaceStaging` (`WorkspaceStagingInterface`) — exposes `getWorkspaceId()`

`Staging\File\FileFormat` (`json` / `yaml`) is the manifest format, passed by the mapping libraries to their
strategies.

Wiring the provider into a reader:

```php
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\StagingProvider\Staging\File\FileFormat;

$strategyFactory = new StrategyFactory($stagingProvider, $clientWrapper, $logger, FileFormat::Json);
$reader = new Reader($clientWrapper, $logger, $strategyFactory);
```

`keboola/output-mapping` has an equivalent `StrategyFactory` used with `TableLoader` / `FileWriter`.

## Workspaces

`Workspace\WorkspaceProvider` wraps the Storage API `Workspaces` and `Components` clients.

```php
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Workspace\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Workspace\Configuration\NewWorkspaceConfig;
use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;

$workspaceProvider = new WorkspaceProvider(
    new Workspaces($storageApiClient),
    new Components($storageApiClient),
    new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
);

$workspace = $workspaceProvider->createNewWorkspace($storageApiToken, new NewWorkspaceConfig(
    stagingType: StagingType::WorkspaceSnowflake,
    componentId: 'keboola.my-component',
    configId: '123',            // null creates a workspace not tied to a configuration
    size: null,
    useReadonlyRole: null,
    networkPolicy: NetworkPolicy::SYSTEM,
    loginType: null,
));

$workspace->getWorkspaceId();
$workspace->getCredentials();
```

| Method | Purpose |
| --- | --- |
| `createNewWorkspace(StorageApiToken, NewWorkspaceConfig)` | Creates a workspace; returns `WorkspaceWithCredentialsInterface` |
| `getExistingWorkspace(string $workspaceId, ?array $credentialsData)` | Loads a workspace; returns `WorkspaceInterface` when `$credentialsData` is `null`, `WorkspaceWithCredentialsInterface` otherwise |
| `resetWorkspaceCredentials(string $workspaceId)` | Issues fresh credentials for a workspace nobody else is using |
| `cleanupWorkspace(string $workspaceId)` | Deletes the workspace; a missing workspace (404) is not an error |

Notes:

* `createNewWorkspace()` first checks the project actually supports the backend (`hasSnowflake` /
  `hasBigquery` on the token owner) and throws `Exception\StagingNotSupportedByProjectException` if not.
  A non-workspace staging type throws `Exception\StagingProviderException`.
* Passing a `configId` routes through `Components::createConfigurationWorkspace()` so the workspace is tied
  to a component configuration; without it a standalone workspace is created. The resulting workspace is
  the same either way.
* For key-pair login (`WorkspaceLoginType`), the provider generates the keypair locally via
  `SnowflakeKeypairGenerator`, sends only the **public** key to Connection, and merges the private key into
  the returned workspace data. The private key is never returned by the API.

### Workspaces with and without credentials

"I have a workspace" and "I can log into it" are separate types rather than a nullable getter:

* `Workspace\WorkspaceInterface` — `getWorkspaceId()`, `getBackendType()`, `getBackendSize()`,
  `getLoginType()`. Enough to talk to the workspace through the Connection API.
* `Workspace\WorkspaceWithCredentialsInterface` — adds `getCredentials()`, needed to connect to the backend
  directly.

Use `getExistingWorkspace($id, null)` when you only need the former; pass a credentials array (for example
credentials supplied by an end user, as with SQL sandboxes) to get the latter. When nobody else holds the
credentials — a staging workspace accessed only from code — `resetWorkspaceCredentials()` is the safe way
to obtain them.

## Development

Run everything through the library's Docker Compose service:

```bash
docker compose run --rm dev-staging-provider composer install
docker compose run --rm dev-staging-provider composer ci     # validate + phpcs + phpstan + tests
```

`tests/Workspace/WorkspaceProviderFunctionalTest.php` creates real workspaces and requires the following
variables in the repository-root `.env` file:

```
STORAGE_API_URL=https://connection.keboola.com
STORAGE_API_TOKEN=...
```

The rest of the test suite runs without them.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
