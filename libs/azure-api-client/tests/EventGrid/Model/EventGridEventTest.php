<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\EventGrid\Model;

use DateTimeImmutable;
use Generator;
use InvalidArgumentException;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use PHPUnit\Framework\TestCase;

class EventGridEventTest extends TestCase
{
    public function testToArray(): void
    {
        $event = new EventGridEvent(
            'event-id',
            'subject',
            ['foo' => 'bar'],
            'Keboola.Test',
            new DateTimeImmutable('2024-03-05T14:15:16.123456+02:00'),
            '2.0',
        );

        self::assertSame([
            'id' => 'event-id',
            'subject' => 'subject',
            'data' => ['foo' => 'bar'],
            'eventType' => 'Keboola.Test',
            'eventTime' => '2024-03-05T12:15:16Z',
            'dataVersion' => '2.0',
        ], $event->toArray());
    }

    public function testDefaults(): void
    {
        $before = new DateTimeImmutable('-1 second');
        $event = new EventGridEvent('event-id', 'subject', [], 'Keboola.Test');
        $after = new DateTimeImmutable('+1 second');

        self::assertSame('1.0', $event->toArray()['dataVersion']);
        self::assertSame('UTC', $event->eventTime->getTimezone()->getName());
        self::assertGreaterThanOrEqual($before, $event->eventTime);
        self::assertLessThanOrEqual($after, $event->eventTime);
    }

    /** @dataProvider provideInvalidArguments */
    public function testInvalidArguments(string $id, string $subject, string $eventType): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @phpstan-ignore-next-line intentionally passing invalid values
        new EventGridEvent($id, $subject, [], $eventType);
    }

    public static function provideInvalidArguments(): Generator
    {
        yield 'empty id' => ['id' => '', 'subject' => 'subject', 'eventType' => 'type'];
        yield 'empty subject' => ['id' => 'id', 'subject' => '', 'eventType' => 'type'];
        yield 'empty eventType' => ['id' => 'id', 'subject' => 'subject', 'eventType' => ''];
    }
}
