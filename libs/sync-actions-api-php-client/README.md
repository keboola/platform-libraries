# Sync Actions PHP Client

PHP client for the Job Queue API ([API docs](https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.0.0)).

## Usage
```bash
composer require keboola/sync-actions-api-php-client
```

```php
use Keboola\SyncActionsClient\Client;
use Keboola\SyncActionsClient\JobData;
use Psr\Log\NullLogger;

$client = new Client(
    'http://sync-actions.keboola.com/',
    'xxx-xxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
);
$result = $client->createJob(new JobData(
    'keboola.ex-db-snowflake',
    'getTables'
));
var_dump($result);
```

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
KBC_SYNC_ACTIONS_URL=https://sync-actions.keboola.com/
KBC_TOKEN=xxx-xxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 
