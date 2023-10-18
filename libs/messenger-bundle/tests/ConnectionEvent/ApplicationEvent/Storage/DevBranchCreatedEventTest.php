<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent\Storage;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchCreatedEvent;
use PHPUnit\Framework\TestCase;

class DevBranchCreatedEventTest extends TestCase
{
    private const EXAMPLE_EVENT = <<<EOT
        {
          "class": "Event_Application",
          "created": "2023-10-18T16:08:48+02:00",
          "data": {
            "name": "storage.devBranchCreated",
            "objectId": 46,
            "idProject": 34,
            "idAdmin": null,
            "idAccount": null,
            "idExport": null,
            "idAccessToken": 107,
            "accessTokenName": "josef.martinec@keboolaconnection.onmicrosoft.com",
            "idBucket": null,
            "idWorkspace": null,
            "tableName": null,
            "objectType": "devBranch",
            "objectName": "123",
            "params": {
              "devBranchName": "123"
            },
            "performance": [],
            "idEvent": 20224549,
            "type": "info",
            "description": "",
            "context": {
              "remoteAddr": null,
              "httpReferer": null,
              "httpUserAgent": null,
              "apiVersion": "v2",
              "userAgent": "Mozilla\/5.0 (X11; Linux x86_64) AppleWebKit\/537.36 (KHTML, like Gecko)",
              "async": true
            },
            "results": [],
            "component": "storage",
            "message": "Development branch \"123\" created",
            "runId": "75576",
            "configurationId": null,
            "fileIds": [],
            "idBranch": null
          }
        }
        EOT;

    public function testDecodeFromRealEvent(): void
    {
        $eventFactory = new ApplicationEventFactory();
        $event = $eventFactory->createEventFromArray(json_decode(self::EXAMPLE_EVENT, true, JSON_THROW_ON_ERROR));

        self::assertInstanceOf(DevBranchCreatedEvent::class, $event);
        self::assertSame(20224549, $event->id);
        self::assertSame(34, $event->projectId);
        self::assertSame(107, $event->accessTokenId);
        self::assertSame('josef.martinec@keboolaconnection.onmicrosoft.com', $event->accessTokenName);
        self::assertSame(46, $event->objectId);
        self::assertSame('devBranch', $event->objectType);
        self::assertSame('123', $event->objectName);
        self::assertSame('Development branch "123" created', $event->message);
        self::assertSame(['devBranchName' => '123'], $event->params);
    }

    public function testCreateFromArray(): void
    {
        $event = DevBranchCreatedEvent::fromArray([
            'name' => 'storage.devBranchCreated',
            'idEvent' => 123,
            'idProject' => 456,
            'idAccessToken' => 789,
            'accessTokenName' => 'test@example.com',
            'objectId' => 741,
            'objectType' => 'devBranch',
            'objectName' => 'testName',
            'message' => 'testMessage',
            'params' => ['testParam' => 'testValue'],
        ]);

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
            'name' => 'storage.devBranchCreated',
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
