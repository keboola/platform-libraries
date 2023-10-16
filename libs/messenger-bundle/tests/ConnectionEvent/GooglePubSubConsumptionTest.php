<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent;

use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Topic;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectDeletedEvent;
use Keboola\MessengerBundle\Tests\TestEnvVarsTrait;
use Symfony\Component\Messenger\Envelope;

class GooglePubSubConsumptionTest extends AbstractQueueConsumptionTest
{
    use TestEnvVarsTrait;

    private readonly Topic $topicClient;

    protected function setUp(): void
    {
        if (self::getRequiredEnv('TEST_CLOUD_PLATFORM') !== 'gcp') {
            $this->markTestSkipped('Only applicable on GCP');
        }

        parent::setUp();

        $dsnPath = parse_url(self::getRequiredEnv('CONNECTION_EVENTS_QUEUE_DSN'), PHP_URL_PATH);
        $topicName = ltrim((string) $dsnPath, '/');
        self::assertNotEmpty($topicName, 'Topic name is empty');

        $pubSubClient = new PubSubClient();
        $this->topicClient = $pubSubClient->topic($topicName);
    }

    protected function publishMessage(array $message): void
    {
        $this->topicClient->publish([
            'data' => json_encode($message, JSON_THROW_ON_ERROR),
        ]);
    }

    public function testMessageDelivery(): void
    {
        // PubSub does not wrap the data
        $wrappedMessage = self::EVENT_DATA;

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
