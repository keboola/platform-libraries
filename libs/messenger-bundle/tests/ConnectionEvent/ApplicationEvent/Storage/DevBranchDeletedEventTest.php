<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent\Storage;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchDeletedEvent;
use PHPUnit\Framework\TestCase;

class DevBranchDeletedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'name' => 'storage.devBranchDeleted',
            'idEvent' => 123,
            'idProject' => 456,
            'idAccessToken' => 789,
            'accessTokenName' => 'test@example.com',
            'objectId' => 741,
            'objectType' => 'devBranch',
            'objectName' => 'testName',
            'message' => 'testMessage',
            'params' => ['testParam' => 'testValue'],
        ],
    ];

    public function testDecodeFromEvent(): void
    {
        $eventFactory = new ApplicationEventFactory();
        $event = $eventFactory->createEventFromArray(self::EVENT_MESSAGE_DATA);

        self::assertInstanceOf(DevBranchDeletedEvent::class, $event);
        self::assertSame(123, $event->id);
    }

    public function testCreateFromArray(): void
    {
        $event = DevBranchDeletedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame(123, $event->id);
        self::assertSame(456, $event->projectId);
        self::assertSame(789, $event->accessTokenId);
        self::assertSame('test@example.com', $event->accessTokenName);
        self::assertSame(741, $event->objectId);
        self::assertSame('devBranch', $event->objectType);
        self::assertSame('testName', $event->objectName);
        self::assertSame('testMessage', $event->message);
        self::assertSame(['testParam' => 'testValue'], $event->params);
    }

    public function testCreateFromArrayWithInvalidName(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchDeletedEvent expects event ' .
            'name "storage.devBranchDeleted" but is "foo"',
        );

        DevBranchDeletedEvent::fromArray([
            'name' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = new DevBranchDeletedEvent(
            id: 123,
            projectId: 456,
            accessTokenId: 789,
            accessTokenName: 'test@example.com',
            objectId: 741,
            objectType: 'devBranch',
            objectName: 'testName',
            message: 'testMessage',
            params: ['testParam' => 'testValue'],
        );

        self::assertSame([
            'name' => 'storage.devBranchDeleted',
            'idEvent' => 123,
            'idProject' => 456,
            'idAccessToken' => 789,
            'accessTokenName' => 'test@example.com',
            'objectId' => 741,
            'objectType' => 'devBranch',
            'objectName' => 'testName',
            'message' => 'testMessage',
            'params' => ['testParam' => 'testValue'],
        ], $event->toArray());
    }
}
