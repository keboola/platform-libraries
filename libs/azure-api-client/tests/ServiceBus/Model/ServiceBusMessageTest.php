<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ServiceBus\Model;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusMessage;
use PHPUnit\Framework\TestCase;

class ServiceBusMessageTest extends TestCase
{
    public function testJson(): void
    {
        $scheduledAt = new DateTimeImmutable('2024-03-05T12:15:16Z');
        $message = ServiceBusMessage::json('message-id', ['foo' => ['bar' => 1]], $scheduledAt);

        self::assertSame('message-id', $message->id);
        self::assertSame('{"foo":{"bar":1}}', $message->body);
        self::assertSame('application/json', $message->contentType);
        self::assertSame($scheduledAt, $message->scheduledEnqueueTime);
    }

    public function testJsonWithoutSchedule(): void
    {
        self::assertNull(ServiceBusMessage::json('message-id', [])->scheduledEnqueueTime);
    }

    public function testEmptyIdIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @phpstan-ignore-next-line intentionally passing invalid values
        new ServiceBusMessage('', 'body', 'text/plain');
    }

    public function testEmptyContentTypeIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @phpstan-ignore-next-line intentionally passing invalid values
        new ServiceBusMessage('id', 'body', '');
    }
}
