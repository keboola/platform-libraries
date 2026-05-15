<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent\Admin;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Admin\AdminRemovedFromProjectEvent;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use PHPUnit\Framework\TestCase;

class AdminRemovedFromProjectEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'name' => 'admin.adminRemovedFromProject',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            'idProject' => 456,
            'idAdmin' => 123,
            'objectId' => 789,
            'objectType' => 'admin',
            'objectName' => 'removed@example.com',
            'params' => ['project' => 'My Project'],
        ],
    ];

    public function testDecodeFromEvent(): void
    {
        $eventFactory = new ApplicationEventFactory();
        $event = $eventFactory->createEventFromArray(self::EVENT_MESSAGE_DATA);

        self::assertInstanceOf(AdminRemovedFromProjectEvent::class, $event);
        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
    }

    public function testCreateFromArray(): void
    {
        $event = AdminRemovedFromProjectEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
        self::assertSame(456, $event->projectId);
        self::assertSame(123, $event->adminId);
        self::assertSame(789, $event->objectId);
        self::assertSame('admin', $event->objectType);
        self::assertSame('removed@example.com', $event->objectName);
        self::assertSame(['project' => 'My Project'], $event->params);
    }

    public function testCreateFromArrayWithInvalidName(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Admin\AdminRemovedFromProjectEvent expects' .
            ' event name "admin.adminRemovedFromProject" but is "foo"',
        );

        AdminRemovedFromProjectEvent::fromArray([
            'name' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = new AdminRemovedFromProjectEvent(
            uuid: '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            projectId: 456,
            adminId: 123,
            objectId: 789,
            objectType: 'admin',
            objectName: 'removed@example.com',
            params: ['project' => 'My Project'],
        );

        self::assertSame([
            'name' => 'admin.adminRemovedFromProject',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
            'idProject' => 456,
            'idAdmin' => 123,
            'objectId' => 789,
            'objectType' => 'admin',
            'objectName' => 'removed@example.com',
            'params' => ['project' => 'My Project'],
        ], $event->toArray());
    }

    public function testGetters(): void
    {
        $event = AdminRemovedFromProjectEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->getId());
        self::assertSame('admin.adminRemovedFromProject', $event->getEventName());
    }
}
