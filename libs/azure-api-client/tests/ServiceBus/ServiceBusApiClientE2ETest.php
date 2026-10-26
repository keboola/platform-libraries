<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ServiceBus;

use DateTimeImmutable;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\ServiceBus\Exception\MessageLockLostException;
use Keboola\AzureApiClient\ServiceBus\Model\ReceivedMessage;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusMessage;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use Keboola\AzureApiClient\Tests\E2ETestEnvironment;
use PHPUnit\Framework\TestCase;

/**
 * @group e2e
 */
class ServiceBusApiClientE2ETest extends TestCase
{
    private const RECEIVE_TIMEOUT_SECONDS = 30;
    private const LOCK_EXPIRY_MARGIN = 5;

    public function testSendReceiveAndDeleteWithSas(): void
    {
        $this->assertSendReceiveAndDelete(E2ETestEnvironment::serviceBusWithSas());
    }

    public function testSendReceiveAndDeleteWithEntraToken(): void
    {
        $this->assertSendReceiveAndDelete(new ServiceBusApiClient(
            E2ETestEnvironment::get('SERVICE_BUS_ENDPOINT'),
            new ApiClientConfiguration(authenticator: E2ETestEnvironment::entraAuth()),
        ));
    }

    public function testScheduledMessageIsDeliveredLater(): void
    {
        $client = E2ETestEnvironment::serviceBusWithSas();
        $queueName = E2ETestEnvironment::get('SERVICE_BUS_QUEUE');
        $messageId = bin2hex(random_bytes(16));
        $scheduledAt = new DateTimeImmutable('+5 seconds');

        $client->sendMessage($queueName, ServiceBusMessage::json($messageId, ['scheduled' => true], $scheduledAt));
        $message = E2ETestEnvironment::receiveMatching(
            $client,
            $queueName,
            fn(ReceivedMessage $message) => $message->brokerProperties->messageId === $messageId,
            self::RECEIVE_TIMEOUT_SECONDS,
        );
        $client->deleteMessage($message->lockLocation);

        self::assertGreaterThanOrEqual($scheduledAt->getTimestamp(), time());
        self::assertNotNull($message->brokerProperties->scheduledEnqueueTimeUtc);
        self::assertSame(
            $scheduledAt->getTimestamp(),
            $message->brokerProperties->scheduledEnqueueTimeUtc->getTimestamp(),
        );
    }

    public function testDeleteAfterLockExpiredRaisesLockLost(): void
    {
        $client = E2ETestEnvironment::serviceBusWithSas();
        $queueName = E2ETestEnvironment::get('SERVICE_BUS_QUEUE');
        $messageId = bin2hex(random_bytes(16));

        $client->sendMessage($queueName, ServiceBusMessage::json($messageId, ['foo' => 'bar']));
        $message = E2ETestEnvironment::receiveMatching(
            $client,
            $queueName,
            fn(ReceivedMessage $message) => $message->brokerProperties->messageId === $messageId,
            self::RECEIVE_TIMEOUT_SECONDS,
        );
        self::assertNotNull($message->brokerProperties->lockedUntilUtc);

        $waitSeconds = $message->brokerProperties->lockedUntilUtc->getTimestamp() - time() + self::LOCK_EXPIRY_MARGIN;
        sleep(max($waitSeconds, 0));

        try {
            $client->deleteMessage($message->lockLocation);
            self::fail('MessageLockLostException was expected');
        } catch (MessageLockLostException $e) {
            self::assertSame(404, $e->getCode());
        }

        // the message is delivered again after the lock expired
        $redelivered = E2ETestEnvironment::receiveMatching(
            $client,
            $queueName,
            fn(ReceivedMessage $message) => $message->brokerProperties->messageId === $messageId,
            self::RECEIVE_TIMEOUT_SECONDS,
        );
        self::assertSame(2, $redelivered->brokerProperties->deliveryCount);
        $client->deleteMessage($redelivered->lockLocation);
    }

    private function assertSendReceiveAndDelete(ServiceBusApiClient $client): void
    {
        $queueName = E2ETestEnvironment::get('SERVICE_BUS_QUEUE');
        $messageId = bin2hex(random_bytes(16));

        $client->sendMessage($queueName, ServiceBusMessage::json($messageId, ['foo' => 'bar']));
        $message = E2ETestEnvironment::receiveMatching(
            $client,
            $queueName,
            fn(ReceivedMessage $message) => $message->brokerProperties->messageId === $messageId,
            self::RECEIVE_TIMEOUT_SECONDS,
        );

        self::assertSame('{"foo":"bar"}', $message->body);
        self::assertSame('application/json', $message->contentType);
        self::assertSame(1, $message->brokerProperties->deliveryCount);
        self::assertNotNull($message->brokerProperties->lockToken);
        self::assertNotNull($message->brokerProperties->lockedUntilUtc);

        $client->deleteMessage($message->lockLocation);

        $this->expectException(MessageLockLostException::class);
        $client->deleteMessage($message->lockLocation);
    }
}
