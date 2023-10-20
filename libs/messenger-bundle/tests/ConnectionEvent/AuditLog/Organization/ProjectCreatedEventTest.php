<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog\Organization;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization\ProjectCreatedEvent;
use PHPUnit\Framework\TestCase;

class ProjectCreatedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'id' => 1234,
            'operation' => 'auditLog.organization.projectCreated',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00Z',
            'admin' => [
                'id' => 456,
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'organization' => [
                    'id' => 1234,
                    'name' => 'organization-name',
                ],
                'project' => [
                    'id' => 4321,
                    'name' => 'project-name',
                ],
            ],
        ],
    ];

    public function testDecodeFromEvent(): void
    {
        $eventFactory = new AuditEventFactory();
        $event = $eventFactory->createEventFromArray(self::EVENT_MESSAGE_DATA);

        self::assertInstanceOf(ProjectCreatedEvent::class, $event);
        self::assertSame('1234', $event->getId());
    }

    public function testCreateFromArray(): void
    {
        $event = ProjectCreatedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('1234', $event->getId());
        self::assertEquals(new DateTimeImmutable('2022-03-03T12:00:00Z'), $event->getCreatedAt());

        self::assertSame('456', $event->getAdminId());
        self::assertSame('admin-name', $event->getAdminName());
        self::assertSame('admin@example.com', $event->getAdminEmail());

        self::assertSame('1234', $event->getOrganizationId());
        self::assertSame('organization-name', $event->getOrganizationName());

        self::assertSame('4321', $event->getProjectId());
        self::assertSame('project-name', $event->getProjectName());
    }

    public function testCreateFromArrayWithInvalidOperation(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization\ProjectCreatedEvent expects event ' .
            'name "auditLog.organization.projectCreated" but operation in data is "foo"',
        );

        ProjectCreatedEvent::fromArray([
            'operation' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = ProjectCreatedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame([
            'id' => '1234',
            'operation' => 'auditLog.organization.projectCreated',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00+0000',
            'admin' => [
                'id' => '456',
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'organization' => [
                    'id' => '1234',
                    'name' => 'organization-name',
                ],
                'project' => [
                    'id' => '4321',
                    'name' => 'project-name',
                ],
            ],
        ], $event->toArray());
    }
}
