<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog\Organization;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization\OrganizationDeletedEvent;
use PHPUnit\Framework\TestCase;

class OrganizationDeletedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'id' => 1234,
            'operation' => 'auditLog.organization.deleted',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00Z',
            'admin' => [
                'id' => 456,
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'organization' => [
                    'id' => 5678,
                    'name' => 'organization-name',
                ],
            ],
        ],
    ];

    public function testDecodeFromEvent(): void
    {
        $eventFactory = new AuditEventFactory();
        $event = $eventFactory->createEventFromArray(self::EVENT_MESSAGE_DATA);

        self::assertInstanceOf(OrganizationDeletedEvent::class, $event);
        self::assertSame('1234', $event->getId());
    }

    public function testCreateFromArray(): void
    {
        $event = OrganizationDeletedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('1234', $event->getId());
        self::assertEquals(new DateTimeImmutable('2022-03-03T12:00:00Z'), $event->getCreatedAt());

        self::assertSame('456', $event->getAdminId());
        self::assertSame('admin-name', $event->getAdminName());
        self::assertSame('admin@example.com', $event->getAdminEmail());

        self::assertSame('5678', $event->getOrganizationId());
        self::assertSame('organization-name', $event->getOrganizationName());
    }

    public function testCreateFromArrayWithInvalidOperation(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization\OrganizationDeletedEvent expects event ' .
            'name "auditLog.organization.deleted" but operation in data is "foo"',
        );

        OrganizationDeletedEvent::fromArray([
            'operation' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = OrganizationDeletedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame([
            'id' => '1234',
            'operation' => 'auditLog.organization.deleted',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00+0000',
            'admin' => [
                'id' => '456',
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'organization' => [
                    'id' => '5678',
                    'name' => 'organization-name',
                ],
            ],
        ], $event->toArray());
    }
}
