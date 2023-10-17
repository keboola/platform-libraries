<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectDeletedEvent;
use Keboola\MessengerBundle\Tests\TestEnvVarsTrait;
use Symfony\Component\Messenger\Envelope;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class AzureServiceBusConsumptionTest extends AbstractQueueConsumptionTest
{
    use TestEnvVarsTrait;

    private HttpClientInterface $serviceBusClient;

    protected function setUp(): void
    {
        if (self::getRequiredEnv('TEST_CLOUD_PLATFORM') !== 'azure') {
            $this->markTestSkipped('Only applicable on Azure');
        }

        parent::setUp();

        $dsnParser = self::getContainer()->get('aymdev_azure_service_bus.dsn_parser');
        $options = $dsnParser->parseDsn(
            self::getRequiredEnv('CONNECTION_AUDIT_LOG_QUEUE_DSN'),
            [],
            'connection_events',
        );

        $httpConfigBuilder = self::getContainer()->get('aymdev_azure_service_bus.http_config_builder');
        $httpClientConfiguration = $httpConfigBuilder->buildSenderConfiguration($options);

        $httpClientFactory = self::getContainer()->get('aymdev_azure_service_bus.http_client_factory');
        $this->serviceBusClient = $httpClientFactory->createClient($httpClientConfiguration);
    }

    protected function publishMessage(array $message): void
    {
        $this->serviceBusClient->request('POST', 'messages', [
            'json' => $message,
        ])->getStatusCode();
    }

    public function testMessageDelivery(): void
    {
        // EventGrid wraps the data
        $wrappedMessage = [
            'data' => self::EVENT_DATA,
        ];

        $this->publishMessage($wrappedMessage);
        $messages = $this->waitAndFetchAllAvailableMessages();

        // result may contain other messages, we need to search for our message
        self::assertGreaterThanOrEqual(1, count($messages));
        $events = array_map(fn(Envelope $envelope) => $envelope->getMessage(), $messages);
        $deleteEvents = array_filter($events, fn(object $event) => $event instanceof ProjectDeletedEvent);
        $deleteEventsIds = array_map(fn(ProjectDeletedEvent $event) => $event->getId(), $deleteEvents);

        self::assertGreaterThanOrEqual(1, count($deleteEvents));
        self::assertContains((string) self::EVENT_DATA['data']['id'], $deleteEventsIds);
    }
}
