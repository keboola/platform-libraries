<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog\Project;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectUndeletedEvent;
use PHPUnit\Framework\TestCase;

class ProjectUndeletedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'id' => 1234,
            'operation' => 'auditLog.project.undeleted',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00Z',
            'admin' => [
                'id' => 456,
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
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

        self::assertInstanceOf(ProjectUndeletedEvent::class, $event);
        self::assertSame('1234', $event->getId());
    }

    public function testCreateFromArray(): void
    {
        $event = ProjectUndeletedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('1234', $event->getId());
        self::assertEquals(new DateTimeImmutable('2022-03-03T12:00:00Z'), $event->getCreatedAt());

        self::assertSame('456', $event->getAdminId());
        self::assertSame('admin-name', $event->getAdminName());
        self::assertSame('admin@example.com', $event->getAdminEmail());

        self::assertSame('4321', $event->getProjectId());
        self::assertSame('project-name', $event->getProjectName());
    }

    public function testCreateFromArrayWithInvalidOperation(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectUndeletedEvent expects event ' .
            'name "auditLog.project.undeleted" but operation in data is "foo"',
        );

        ProjectUndeletedEvent::fromArray([
            'operation' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = ProjectUndeletedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame([
            'id' => '1234',
            'operation' => 'auditLog.project.undeleted',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00+0000',
            'admin' => [
                'id' => '456',
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'project' => [
                    'id' => '4321',
                    'name' => 'project-name',
                ],
            ],
        ], $event->toArray());
    }
}
