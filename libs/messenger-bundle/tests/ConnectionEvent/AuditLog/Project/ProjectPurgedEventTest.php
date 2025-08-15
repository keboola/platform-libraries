<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog\Project;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectPurgedEvent;
use PHPUnit\Framework\TestCase;

class ProjectPurgedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'id' => 1234,
            'operation' => 'auditLog.project.purged',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00Z',
            'admin' => null,

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

        self::assertInstanceOf(ProjectPurgedEvent::class, $event);
        self::assertSame('1234', $event->getId());
    }

    public function testCreateFromArray(): void
    {
        $event = ProjectPurgedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('1234', $event->getId());
        self::assertEquals(new DateTimeImmutable('2022-03-03T12:00:00Z'), $event->getCreatedAt());

        self::assertNull($event->getAdminId());
        self::assertNull($event->getAdminName());
        self::assertNull($event->getAdminEmail());

        self::assertSame('4321', $event->getProjectId());
        self::assertSame('project-name', $event->getProjectName());
    }

    public function testCreateFromArrayWithInvalidOperation(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectPurgedEvent expects event ' .
            'name "auditLog.project.purged" but operation in data is "foo"',
        );

        ProjectPurgedEvent::fromArray([
            'operation' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = ProjectPurgedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame([
            'id' => '1234',
            'operation' => 'auditLog.project.purged',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00+0000',
            'admin' => null,
            'context' => [
                'project' => [
                    'id' => '4321',
                    'name' => 'project-name',
                ],
            ],
        ], $event->toArray());
    }
}
