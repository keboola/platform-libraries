# Vault API Client

PHP client for the Keboola Vault API, built on top of `keboola/php-api-client-base`.

## Installation

```bash
composer require keboola/vault-api-client
```

## Usage

```php
use Keboola\VaultApiClient\Variables\Model\ListOptions;
use Keboola\VaultApiClient\Variables\Model\Variable;
use Keboola\VaultApiClient\Variables\VariablesApiClient;

$client = new VariablesApiClient(
    baseUrl: 'https://vault.keboola.com',
    token: 'your-storage-api-token',
);

// Create a variable
$variable = $client->createVariable(
    key: 'MY_SECRET',
    value: 'secret-value',
    flags: [Variable::FLAG_ENCRYPTED],
    attributes: ['branchId' => '123'],
);

// List variables
$variables = $client->listVariables(new ListOptions(offset: 0, limit: 50));

// List scoped variables for a branch
$branchVariables = $client->listScopedVariablesForBranch(branchId: '123');

// Delete a variable
$client->deleteVariable(hash: $variable->hash);
```

### Custom configuration

Pass retry, timeout, and logging options directly as constructor parameters:

```php
use Monolog\Logger;

$client = new VariablesApiClient(
    baseUrl: 'https://vault.keboola.com',
    token: 'your-storage-api-token',
    backoffMaxTries: 3,
    logger: new Logger('vault'),
);
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
