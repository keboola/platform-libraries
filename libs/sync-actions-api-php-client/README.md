# Sync Actions PHP Client

PHP client for the Keboola sync actions API, built on top of
[`keboola/php-api-client-base`](../php-api-client-base).

## Usage
```bash
composer require keboola/sync-actions-client
```

```php
use Keboola\SyncActionsClient\ActionData;
use Keboola\SyncActionsClient\SyncActionsApiClient;

$client = new SyncActionsApiClient(
    'https://sync-actions.keboola.com/',
    'xxx-xxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
);

// Invoke a component action; $response->data is a stdClass with the raw action payload.
$response = $client->callAction(new ActionData(
    'keboola.ex-db-snowflake',
    'getTables',
));
var_dump($response->data);

// List the actions a component exposes.
$actions = $client->getActions('keboola.ex-db-snowflake');
var_dump($actions->actions);
```

Failures throw `Keboola\SyncActionsClient\Exception\SyncActionsClientException`.

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/sync-actions-api-php-client
cd sync-actions-api-php-client
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Create `.env.local` file with following contents:

```shell
HOSTNAME_SUFFIX=keboola.com
STORAGE_API_TOKEN=xxx-xxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)
