<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog\Admin;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Admin\AdminDeletedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use PHPUnit\Framework\TestCase;

class AdminDeletedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'id' => 1234,
            'operation' => 'auditLog.admin.deleted',
            'auditLogEventCreatedAt' => '2025-09-03T12:00:00Z',
            'admin' => [
                'id' => 456,
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'admin' => [
                    'id' => 789,
                    'name' => 'subject-admin',
                    'email' => 'subject-admin@keboola.com',
                ],
            ],
        ],
    ];

    public function testDecodeFromEvent(): void
    {
        $eventFactory = new AuditEventFactory();
        $event = $eventFactory->createEventFromArray(self::EVENT_MESSAGE_DATA);

        self::assertInstanceOf(AdminDeletedEvent::class, $event);
        self::assertSame('1234', $event->getId());
    }

    public function testCreateFromArray(): void
    {
        $event = AdminDeletedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('1234', $event->getId());
        self::assertEquals(new DateTimeImmutable('2025-09-03T12:00:00Z'), $event->getCreatedAt());

        self::assertSame('456', $event->getAdminId());
        self::assertSame('admin-name', $event->getAdminName());
        self::assertSame('admin@example.com', $event->getAdminEmail());

        self::assertSame('789', $event->getSubjectAdminId());
        self::assertSame('subject-admin', $event->getSubjectAdminName());
        self::assertSame('subject-admin@keboola.com', $event->getSubjectAdminEmail());
    }

    public function testCreateFromArrayWithInvalidOperation(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\AuditLog\Admin\AdminDeletedEvent expects event ' .
            'name "auditLog.admin.deleted" but operation in data is "foo"',
        );

        AdminDeletedEvent::fromArray([
            'operation' => 'foo',
        ]);
    }
}
