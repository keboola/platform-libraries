<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent\Storage;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchCreatedEvent;
use PHPUnit\Framework\TestCase;

class DevBranchCreatedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'name' => 'storage.devBranchCreated',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
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

        self::assertInstanceOf(DevBranchCreatedEvent::class, $event);
        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
    }

    public function testCreateFromArray(): void
    {
        $event = DevBranchCreatedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
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
            'Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchCreatedEvent expects event' .
            ' name "storage.devBranchCreated" but is "foo"',
        );

        DevBranchCreatedEvent::fromArray([
            'name' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = new DevBranchCreatedEvent(
            uuid: '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
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
            'name' => 'storage.devBranchCreated',
            'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
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
