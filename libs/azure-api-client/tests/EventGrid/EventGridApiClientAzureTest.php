<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\EventGrid;

use DateTimeImmutable;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\CustomHeaderAuth;
use Keboola\AzureApiClient\EventGrid\EventGridApiClient;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\ServiceBus\Model\ReceivedMessage;
use Keboola\AzureApiClient\Tests\AzureTestEnvironment;
use PHPUnit\Framework\TestCase;

/**
 * Publishes to the test topic and reads the event back from the Service Bus queue the topic delivers to.
 *
 * @group azure
 */
class EventGridApiClientAzureTest extends TestCase
{
    private const DELIVERY_TIMEOUT_SECONDS = 60;

    public function testPublishWithAccessKey(): void
    {
        $this->assertEventIsDelivered(new EventGridApiClient(
            AzureTestEnvironment::get('EVENT_GRID_TOPIC_HOSTNAME'),
            new ApiClientConfiguration(
                authenticator: new CustomHeaderAuth('aeg-sas-key', AzureTestEnvironment::get('EVENT_GRID_ACCESS_KEY')),
            ),
        ));
    }

    public function testPublishWithEntraToken(): void
    {
        $this->assertEventIsDelivered(new EventGridApiClient(
            AzureTestEnvironment::get('EVENT_GRID_TOPIC_HOSTNAME'),
            new ApiClientConfiguration(authenticator: AzureTestEnvironment::entraTokenAuth()),
        ));
    }

    private function assertEventIsDelivered(EventGridApiClient $client): void
    {
        $eventId = bin2hex(random_bytes(16));
        $eventTime = new DateTimeImmutable('2024-03-05T12:15:16Z');

        $client->publishEvents([
            new EventGridEvent($eventId, 'TestSubject', ['foo' => 'bar'], 'Keboola.AzureApiClient.Test', $eventTime),
        ]);

        $serviceBus = AzureTestEnvironment::serviceBusWithSas();
        $message = AzureTestEnvironment::receiveMatching(
            $serviceBus,
            AzureTestEnvironment::get('EVENT_GRID_QUEUE'),
            fn(ReceivedMessage $message) => (Json::decodeArray($message->body)['id'] ?? null) === $eventId,
            self::DELIVERY_TIMEOUT_SECONDS,
        );
        $serviceBus->deleteMessage($message->lockLocation);

        $event = Json::decodeArray($message->body);
        self::assertSame('TestSubject', $event['subject']);
        self::assertSame(['foo' => 'bar'], $event['data']);
        self::assertSame('Keboola.AzureApiClient.Test', $event['eventType']);
        self::assertSame('1.0', $event['dataVersion']);
        self::assertEquals($eventTime, new DateTimeImmutable($event['eventTime']));
        self::assertStringEndsWith(
            '/providers/Microsoft.EventGrid/topics/' . AzureTestEnvironment::get('EVENT_GRID_TOPIC_NAME'),
            $event['topic'],
        );
    }
}
