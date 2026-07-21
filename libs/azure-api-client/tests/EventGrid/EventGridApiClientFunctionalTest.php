<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\EventGrid;

use DateTimeImmutable;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\CustomHeaderAuth;
use Keboola\AzureApiClient\Authentication\Authenticator\SASTokenAuthenticatorFactory;
use Keboola\AzureApiClient\EventGrid\EventGridApiClient;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\TestCase;

/**
 * @group functional
 */
class EventGridApiClientFunctionalTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    private function getClient(): EventGridApiClient
    {
        return new EventGridApiClient(
            (string) getenv('AZURE_API_CLIENT_CI__EVENT_GRID__TOPIC_HOSTNAME'),
            new ApiClientConfiguration(
                authenticator: new CustomHeaderAuth(
                    header: 'aeg-sas-key',
                    value: (string) getenv('AZURE_API_CLIENT_CI__EVENT_GRID__ACCESS_KEY'),
                ),
            )
        );
    }

    private function getServiceBusClient(): ServiceBusApiClient
    {
        $endpoint = (string) getenv('AZURE_API_CLIENT_CI__SERVICE_BUS__ENDPOINT');
        return new ServiceBusApiClient(
            //@phpstan-ignore-next-line
            serviceBusEndpoint: $endpoint,
            configuration: new ApiClientConfiguration(
                authenticator: new SASTokenAuthenticatorFactory(
                    url: $endpoint,
                    sharedAccessKeyName: 'RootManageSharedAccessKey',
                    sharedAccessKey: (string) getenv('AZURE_API_CLIENT_CI__SERVICE_BUS__SHARED_ACCESS_KEY'),
                ),
            )
        );
    }

    public function testPublishEvent(): void
    {
        $client = $this->getClient();
        $client->publishEvents([
            new EventGridEvent(
                '3e9825ed-b7db-44ea-a5c9-a1601fa43e23',
                'TestSubject',
                [
                    'Property1' => 'Value1',
                    'Property2' => 'Value2',
                ],
                'Keboola.EventGridClient.TestEvent'
            ),
        ]);

        $serviceBusClient = $this->getServiceBusClient();
        $message = $serviceBusClient->peakMessage('queue-tests');
        $this->assertNotNull($message);
        // returned message has id assigned by service bus
        self::assertNotSame('3e9825ed-b7db-44ea-a5c9-a1601fa43e23', $message->id);
        $messageBody = Json::decodeArray($message->body);
        self::assertSame('3e9825ed-b7db-44ea-a5c9-a1601fa43e23', $messageBody['id']);
        self::assertSame('TestSubject', $messageBody['subject']);
        self::assertSame([
            'Property1' => 'Value1',
            'Property2' => 'Value2',
        ], $messageBody['data']);
        self::assertSame('Keboola.EventGridClient.TestEvent', $messageBody['eventType']);
        self::assertSame('1.0', $messageBody['dataVersion']);
        self::assertSame('1', $messageBody['metadataVersion']);
        self::assertEqualsWithDelta(
            (new DateTimeImmutable('now'))->getTimestamp(),
            (new DateTimeImmutable($messageBody['eventTime']))->getTimestamp(),
            10 // peak has timeout 10s
        );
        self::assertStringEndsWith(
            'Microsoft.EventGrid/topics/' . getenv('AZURE_API_CLIENT_CI__EVENT_GRID__TOPIC_NAME'),
            $messageBody['topic']
        );
        $serviceBusClient->deleteMessage($message->lockLocation);
    }
}
