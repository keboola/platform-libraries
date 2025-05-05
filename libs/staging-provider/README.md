# Staging Provider

[![Build Status](https://dev.azure.com/keboola-dev/wokspace-provider/_apis/build/status/keboola.staging-provider?branchName=main)](https://dev.azure.com/keboola-dev/wokspace-provider/_build/latest?definitionId=69&branchName=main)

## Installation

`composer require keboola/staging-provider`

## Usage

The staging provider package helps you to properly configure input/output staging factory for various environments.

Typical use-case can be set up a `Reader` instance to access some data:

```php
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StagingProvider\Provider\ExistingWorkspaceProvider;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

$storageApiClient = new Client(...);
$storageApiClientWrapper = new ClientWrapper($storageApiClient, ...);
$logger = new NullLogger();

$strategyFactory = new InputStrategyFactory($storageApiClientWrapper, $logger, 'json');
$tokenInfo = $storageApiClient->verifyToken();
$dataDir = '/data';

$workspaceProvider = new ExistingWorkspaceProvider(
    new Workspaces($storageApiClient),
    'my-workspace', // workspace ID
    new Credentials\ExistingCredentialsProvider(
        new Configuration\WorkspaceCredentials([
            'password' => 'abcd1234' // workspace password
        ]),
    ),
);

$providerInitializer = new InputProviderInitializer($strategyFactory, $workspaceProvider, $dataDir);
$providerInitializer->initializeProviders(
    InputStrategyFactory::WORKSPACE_SNOWFLAKE,
    $tokenInfo
);

// now the $strategyFactory is ready to be used
$reader = new Reader($strategyFactory);
```

We start by creating a `StrategyFactory` needed by the reader. The strategy itself has no knowledge of which storage
should be used with each staging type. This is what provider initializer does - configure the `StrategyFactory` for
a specific type of staging.

To create a provider initializer, we pass it:
* the `StrategyFactory` to initialize
* a workspace provider, used to access workspace information for workspace staging
  * `ExistingWorkspaceProvider` in case we want to re-use existing workspace
  * `NewWorkspaceProvider` in case we want a new workspace to be created (based on a component configuration)
* a data directory path used for local staging

Then we call `initializeProviders` method to configure the `StrategyFactory` for specific staging type.  It's up to the
caller to know, which staging type to configure:
* when working with components, each component has staging type defined in its configuration
* sandbox has the type deduced from its workspace
* etc.

The example above presents usage of `InputProviderInitializer` for configuration of input mapping `StrategyFactory` for
a `Reader`. Similarly, we can use `OutputProviderInitializer` to configure output mapping `StrategyFactory` for a `Writer`. 

## Internals
The main objective of the library is to configure `StrategyFactory` so it knows which staging provider to
use with each kind of storage.

### Staging
Generally, there are two kinds of staging:
* local staging - used to store data locally on filesystem, represented by `LocalStaging` class
* workspace staging - used to store data in a workspace, represented by `WorkspaceStagingInterface`

### Provider (staging provider)
The `StrategyFactory` does not use a staging directly but rather through a provider (`ProviderInterface`) so there is
a provider implementation for each kind:
* `LocalStagingProvider` - for local filesystem staging
* `WorkspaceProviderInterface` - for Connection workspace staging
  
The main reason the `StrategyFactory` does not use the staging directly is to achieve lazy initialization of the staging -
provider instance is created during bootstrap, but the staging instance is only created when really used.

### Workspace provider factory
Local staging is pretty simple. It contains just the path to the data directory, provided by the caller. On the other hand,
things get a bit more complicated with workspace staging as the provider may represent an already existing workspace or
a configuration for creating a new workspace. To achieve this, caller must provide a `WorkspaceProviderInterface`.
Currently, there are 2 implementations:
* `NewWorkspaceProvider` which creates a provider that creates a new workspace based on a component configuration
* `ExistingWorkspaceProvider` which creates a provider working with an existing workspace

When using `ExistingWorkspaceProvider`, a developer is responsible for providing workspace credentials. Depending on the
situation, the following options are available:
* `ExistingCredentialsProvider` for situation when we know the exact workspace credentials. For example, when working with
  a workspace for which the end user provides credentials, like SQL sandbox. Credentials are in the form of a free array,
  and it's the caller's responsibility to provide correct credentials properties (password, private key, etc.).
* `ResetCredentialsProvider` for situation nobody else accesses the workspace, and we can safely generate new credentials.
  This is typical when working with a staging workspace, which is accessed only through code (nobody has stored the credentials
  anywhere).
* `NoCredentialsProvider` for situations when we need to just work with the workspace indirectly (through Connection API)
  and don't need credentials. When something tries to access the credentials, the provider throws an exception.
  
## Development
First start with creating `.env` file from `.env.dist`.
```bash
cp .env.dist .env
# edit .env to set variable values
```

To run tests, there is a separate service for each PHP major version (5.6 to 7.4).
For example, to run tests against PHP 5.6, run following:
```bash
docker compose run --rm tests56
```

To develop locally, use `dev` service. Following will install Composer dependencies:
```bash
docker compose run --rm dev composer install
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
