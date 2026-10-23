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

```bash
docker compose run --rm dev-azure-api-client composer install
docker compose run --rm dev-azure-api-client composer ci
```

### Azure resources
Tests in the `azure` PHPUnit group run against real Service Bus and Event Grid resources and are not part of
`composer ci`. Provision them with Terraform, logged in with `az login` to an identity that can create resources and
role assignments in the subscription (Owner, or Contributor + User Access Administrator). The identity passed as
`test_principal_object_id` gets the Service Bus and Event Grid data roles; the Entra token tests run as the identity
logged in to the Azure CLI, so locally pass your own user:

```bash
cd libs/azure-api-client/provisioning
terraform init
terraform apply -var name_prefix=<your-nickname> -var az_tenant_id=<tenant-id> \
  -var az_subscription_id=<subscription-id> -var az_location=westeurope \
  -var test_principal_object_id="$(az ad signed-in-user show --query id -o tsv)"
./write-env.sh   # writes AZURE_API_CLIENT_TEST__* variables to the monorepo root .env
```

`write-env.sh` also stores Entra tokens of the logged-in identity, which expire after about an hour; run it again
before a later test run.

Then run the group:

```bash
docker compose run --rm dev-azure-api-client composer tests-azure
```

Role assignments can take a few minutes to propagate after the first `apply`; until then the Entra token tests fail
with 401. Remove the resources with `terraform destroy` with the same variables.

The Terraform state is kept locally in `provisioning/` and contains the keys in plain text; it is gitignored, keep it that way.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
