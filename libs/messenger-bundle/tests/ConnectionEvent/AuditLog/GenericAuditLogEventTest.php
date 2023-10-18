<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\GenericAuditLogEvent;
use PHPUnit\Framework\TestCase;

class GenericAuditLogEventTest extends TestCase
{
    private const TEST_DATA = [
        'id' => '1234',
        'operation' => 'unknown.event',
        'foo' => 'bar',
    ];

    public function testGetEventName(): void
    {
        $event = new GenericAuditLogEvent([]);
        self::assertSame('genericEvent', $event->getEventName());
    }

    public function testGetData(): void
    {
        $event = new GenericAuditLogEvent(self::TEST_DATA);
        self::assertSame(self::TEST_DATA, $event->getData());
    }

    public function testCreateFromArray(): void
    {
        $event = GenericAuditLogEvent::fromArray(self::TEST_DATA);
        self::assertSame(self::TEST_DATA, $event->getData());
    }

    public function testToArray(): void
    {
        $event = new GenericAuditLogEvent(self::TEST_DATA);
        self::assertSame(self::TEST_DATA, $event->toArray());
    }

    public function testGetId(): void
    {
        $event = new GenericAuditLogEvent(self::TEST_DATA);
        self::assertSame('1234', $event->getId());
    }

    public function testGetIdWithoutId(): void
    {
        $event = new GenericAuditLogEvent([]);
        self::assertSame('', $event->getId());
    }
}
