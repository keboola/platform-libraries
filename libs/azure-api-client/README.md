# Azure API Client

## Installation
```bash
composer require keboola/azure-api-client
```

## Usage
The simplest way to use API client is just by creating its instance. 

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Monolog\Logger;

$logger = new Logger('azure-api-client');
$marketplaces = new MarketplaceApiClient(new ClientCredentialsAuth(
    logger: $logger,
));
```

### Authentication
By default, API client will try to authenticate using ENV variables `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and
`AZURE_CLIENT_SECRET`. If some of them is not available, it'll fall back to Azure metadata API.

If you want to supply your own credentials, you can pass custom authenticator instance in the client options:

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\ClientCredentialsAuth;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;

$marketplaces = new MarketplaceApiClient(new ApiClientConfiguration(
    authenticator: new ClientCredentialsAuth(
        'tenant-id',
        'client-id',
        'client-secret',
    ),
));
```

Or can provide custom authentication token directly:

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\StaticBearerTokenAuth;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;

$marketplaces = new MarketplaceApiClient(new ApiClientConfiguration(
    authenticator: new StaticBearerTokenAuth('my-token'),
));
```

Or even use custom authentication header if needed (this is how an Event Grid topic access key is sent):

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\CustomHeaderAuth;
use Keboola\AzureApiClient\EventGrid\EventGridApiClient;

$eventGrid = new EventGridApiClient('<topic>.<region>-1.eventgrid.azure.net', new ApiClientConfiguration(
    authenticator: new CustomHeaderAuth('aeg-sas-key', '<topic-access-key>'),
));
```

Services that accept Shared Access Signatures (Service Bus) can be authenticated with a shared access policy:

```php
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\SharedAccessSignatureAuth;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;

$serviceBus = new ServiceBusApiClient('https://<namespace>.servicebus.windows.net:443/', new ApiClientConfiguration(
    authenticator: new SharedAccessSignatureAuth('RootManageSharedAccessKey', '<shared-access-key>'),
));
```

Each request gets its own token, scoped to the request URI and valid for an hour (configurable by the third
argument).

If even this is not enough for your use-case, you can implement your own
`Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorFactoryInterface` and pass it as `authenticator`. 

### Timeouts and retries
Requests time out after 120 s (10 s to connect) and failed requests (5xx, 429, connection errors) are retried up to
10 times. All of it is set in `ApiClientConfiguration`:

```php
new ApiClientConfiguration(
    backoffMaxTries: 0,     // the caller retries on its own
    connectTimeout: 10,
    requestTimeout: 20,
);
```

The defaults suit occasional calls, not a message consumer: with 10 retries and exponential backoff a single outage
can block a call for many minutes, well past a Service Bus lock duration. Consumers should keep the whole retry budget
below the lock duration and pass a real logger, retries are only visible as its warnings:

```php
new ApiClientConfiguration(
    backoffMaxTries: 3,
    logger: $logger,
    connectTimeout: 10,
    requestTimeout: 20,
);
```

Retries resend requests whose outcome is unknown:

- `ServiceBusApiClient::sendMessage()` and `EventGridApiClient::publishEvents()` can deliver a message or event twice,
  so receivers must tolerate duplicates (a Service Bus queue can also deduplicate by message id).
- `ServiceBusApiClient::deleteMessage()` retried after a lost response finds the message already gone, and raises
  `MessageLockLostException` although the first attempt deleted it.

Redirects are not followed: the request would be authenticated again for the redirect target.

### Service Bus
Send, peek-lock receive and delete (complete) of queue messages over the
[Service Bus REST API](https://learn.microsoft.com/en-us/rest/api/servicebus/service-bus-runtime-rest).

```php
use Keboola\AzureApiClient\ServiceBus\Exception\MessageLockLostException;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusMessage;

$serviceBus->sendMessage('my-queue', ServiceBusMessage::json('message-id', ['foo' => 'bar']));
$serviceBus->sendMessage('my-queue', ServiceBusMessage::json(
    'delayed-message-id',
    ['foo' => 'bar'],
    scheduledEnqueueTime: new DateTimeImmutable('+5 minutes'),
));

$message = $serviceBus->peekLockMessage('my-queue', waitSeconds: 10); // null when the queue stays empty
if ($message !== null) {
    process($message->body, $message->brokerProperties->deliveryCount);
    try {
        $serviceBus->deleteMessage($message->lockLocation);
    } catch (MessageLockLostException) {
        // the lock expired before the delete, the message will be delivered again
    }
}
```

`waitSeconds` has to be shorter than `requestTimeout`, otherwise the HTTP request would time out before Service Bus
answers.

Without an explicit authenticator the client uses an Entra token for `https://servicebus.azure.net` (see
[Authentication](#authentication)); the identity needs the *Azure Service Bus Data Sender* role to send and *Azure Service Bus Data Receiver* to receive and
delete.

### Event Grid
Publishes events in the [Event Grid schema](https://learn.microsoft.com/en-us/azure/event-grid/event-schema) to a
topic.

```php
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;

$eventGrid->publishEvents([
    new EventGridEvent(
        id: 'event-id',
        subject: 'subject',
        data: ['foo' => 'bar'],
        eventType: 'MyApp.SomethingHappened',
    ),
]);
```

Without an explicit authenticator the client uses an Entra token for `https://eventgrid.azure.net`; the identity
needs the *EventGrid Data Sender* role.

## Development

### Azure resources
Tests in the `e2e` PHPUnit group run against real Service Bus and Event Grid resources. They are part of
`composer ci`, so create the resources before running it. Log in to the Azure CLI (`az login`) with an identity that can
create resources, service principals and role assignments in the Keboola DEV Platform Services Team subscription.
Terraform keeps its state in the shared local-dev S3 bucket, so you also need AWS credentials that can assume the
`kbc-local-dev-terraform` role.

```bash
export NAME_PREFIX= # your name/nickname to make your resource unique & recognizable

cat <<EOF > ./provisioning/local/terraform.tfvars
name_prefix = "${NAME_PREFIX}"
EOF

terraform -chdir=./provisioning/local init -backend-config="key=azure-api-client/${NAME_PREFIX}.tfstate"
terraform -chdir=./provisioning/local apply
./provisioning/local/update-env.sh

docker compose run --rm dev-azure-api-client composer install
docker compose run --rm dev-azure-api-client composer ci
```

`update-env.sh` writes the `AZURE_API_CLIENT_TEST__*` variables to `.env.local`. The Entra token tests run as a
service principal Terraform creates, with the same data roles a production sender and receiver get. Role assignments
can take a few minutes to propagate after the first `apply`; until then those tests fail with 401.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
