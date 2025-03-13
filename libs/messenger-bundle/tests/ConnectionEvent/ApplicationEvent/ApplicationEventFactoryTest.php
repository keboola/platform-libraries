<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent;

use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\GenericApplicationEvent;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchCreatedEvent;
use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;
use PHPUnit\Framework\TestCase;

class ApplicationEventFactoryTest extends TestCase
{
    public static function provideInvalidData(): iterable
    {
        yield 'no "data"' => [
            'data' => [],
            'error' => 'Missing or invalid property "data"',
        ];

        yield 'no "data.name"' => [
            'data' => [
                'data' => [],
            ],
            'error' => 'Missing property "data.name"',
        ];
    }

    /** @dataProvider provideInvalidData */
    public function testInvalidData(array $data, string $expectedError): void
    {
        $factory = new ApplicationEventFactory();

        $this->expectException(EventFactoryException::class);
        $this->expectExceptionMessage($expectedError);

        $factory->createEventFromArray($data);
    }

    public function testErrorInDataMapping(): void
    {
        $factory = new ApplicationEventFactory();

        $this->expectException(EventFactoryException::class);
        $this->expectExceptionMessage(
            'Failed to create Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\GenericApplicationEvent ' .
            'from event data: Event is missing property "uuid"',
        );

        $factory->createEventFromArray([
            'data' => [
                'name' => 'some.event',
            ],
        ]);
    }

    public function testCreateRegularEvent(): void
    {
        $factory = new ApplicationEventFactory();

        $event = $factory->createEventFromArray([
            'class' => 'Event_Application',
            'created' => '2023-10-18T16:08:48+02:00',
            'data' => [
                'name' => 'storage.devBranchCreated',
                'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
                'idProject' => 34,
                'idAccessToken' => 107,
                'accessTokenName' => 'josef.martinec@keboolaconnection.onmicrosoft.com',
                'objectId' => 46,
                'objectType' => 'devBranch',
                'objectName' => '123',
                'params' => [
                    'devBranchName' => '123',
                ],
                'message' => 'Development branch "123" created',
            ],
        ]);

        self::assertInstanceOf(DevBranchCreatedEvent::class, $event);
        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
    }

    public function testCreateGenericEvent(): void
    {
        $factory = new ApplicationEventFactory();

        $event = $factory->createEventFromArray([
            'class' => 'Event_Application',
            'created' => '2023-10-17T10:29:33+02:00',
            'data' => [
                'name' => 'unknown.event',
                'objectId' => '',
                'idProject' => 18,
                'params' => [
                    'task' => 'file-import',
                ],
                'uuid' => '01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d',
                'type' => 'info',
            ],
        ]);

        self::assertInstanceOf(GenericApplicationEvent::class, $event);
        self::assertSame('unknown.event', $event->name);
        self::assertSame('', $event->objectId);
        self::assertSame(18, $event->idProject);
        self::assertSame(['task' => 'file-import'], $event->params);
        self::assertSame('01958fcb-6cf6-778b-ac16-fb7cd4f9ab3d', $event->uuid);
        self::assertSame('info', $event->type);
    }
}
