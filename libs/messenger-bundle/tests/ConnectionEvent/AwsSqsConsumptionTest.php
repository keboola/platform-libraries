<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent;

use AsyncAws\Sqs\Input\SendMessageRequest;
use AsyncAws\Sqs\SqsClient;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectDeletedEvent;
use Keboola\MessengerBundle\Tests\TestEnvVarsTrait;
use Symfony\Component\Messenger\Envelope;

class AwsSqsConsumptionTest extends AbstractQueueConsumptionTest
{
    use TestEnvVarsTrait;

    private SqsClient $sqsClient;

    protected function setUp(): void
    {
        if (self::getRequiredEnv('TEST_CLOUD_PLATFORM') !== 'aws') {
            $this->markTestSkipped('Only applicable on AWS');
        }

        parent::setUp();
        $this->sqsClient = new SqsClient();
    }

    protected function publishMessage(array $message): void
    {
        $this->sqsClient->sendMessage(new SendMessageRequest([
            'QueueUrl' => self::getRequiredEnv('CONNECTION_EVENTS_QUEUE_DSN'),
            'MessageBody' => json_encode($message, JSON_THROW_ON_ERROR),
        ]))->getMessageId();
    }

    public function testMessageDelivery(): void
    {
        // SNS wraps the data
        $wrappedMessage = [
            'Type' => 'Notification',
            'MessageId' => '2e6b05e6-b732-5c68-a199-5f43ac83b847',
            'TopicArn' => 'arn:aws:sns:eu-west-1:932185800179:kbc-connection-services-AuditLogTopic-P6SIF3FERKK6',
            'Message' => json_encode(self::EVENT_DATA, JSON_THROW_ON_ERROR),
            'Timestamp' => '2022-03-11T09:42:09.219Z',
            'SignatureVersion' => '1',
            'Signature' => 'kPMaQDxM/...fXDJIon8PemSbA==',
            'SigningCertURL' => 'https://sns.eu-west-1.amazonaws.com/SimpleNotificationService...',
            'UnsubscribeURL' => 'https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&Subsc...',
            'MessageAttributes' => [
                'eventName' => [
                    'Type' => 'String',
                    'Value' => 'auditLog.maintainers.listed',
                ],
            ],
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
