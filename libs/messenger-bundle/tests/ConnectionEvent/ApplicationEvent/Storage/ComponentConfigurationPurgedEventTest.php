<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent\Storage;

use InvalidArgumentException;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\ComponentConfigurationPurgedEvent;
use PHPUnit\Framework\TestCase;

class ComponentConfigurationPurgedEventTest extends TestCase
{
    private const EVENT_MESSAGE_DATA = [
        'data' => [
            'name' => 'storage.componentConfigurationPurged',
            'uuid' => 'uuid-123',
            'idProject' => 123,
            'idAccessToken' => 456,
            'accessTokenName' => 'access-token-name',
            'objectId' => 'object-id-123',
            'objectType' => 'configuration',
            'objectName' => 'My Kafka Data Destination',
            'message' => 'Component configuration purged',
            'params' => [
                'component' => 'keboola.wr-kafka',
                'configurationId' => '01k0vea53rhcw3anmg57w2k2aj',
                'name' => 'My Kafka Data Destination',
                'version' => 6,
            ],
            'idBranch' => 789,
        ],
    ];

    public function testDecodeFromEvent(): void
    {
        $eventFactory = new ApplicationEventFactory();
        $event = $eventFactory->createEventFromArray(self::EVENT_MESSAGE_DATA);

        self::assertInstanceOf(ComponentConfigurationPurgedEvent::class, $event);
        self::assertSame('uuid-123', $event->getId());
    }

    public function testCreateFromArray(): void
    {
        $event = ComponentConfigurationPurgedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame('uuid-123', $event->uuid);
        self::assertSame(123, $event->projectId);
        self::assertSame(456, $event->accessTokenId);
        self::assertSame('access-token-name', $event->accessTokenName);
        self::assertSame('object-id-123', $event->objectId);
        self::assertSame('configuration', $event->objectType);
        self::assertSame('My Kafka Data Destination', $event->objectName);
        self::assertSame('Component configuration purged', $event->message);
        self::assertSame([
            'component' => 'keboola.wr-kafka',
            'configurationId' => '01k0vea53rhcw3anmg57w2k2aj',
            'name' => 'My Kafka Data Destination',
            'version' => 6,
        ], $event->params);
        self::assertSame(789, $event->idBranch);
        self::assertSame('keboola.wr-kafka', $event->getComponent());
        self::assertSame('01k0vea53rhcw3anmg57w2k2aj', $event->getConfigurationId());
    }

    public function testCreateFromArrayWithInvalidName(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\ComponentConfigurationPurgedEvent ' .
            'expects event name "storage.componentConfigurationPurged" but is "foo"',
        );

        ComponentConfigurationPurgedEvent::fromArray([
            'name' => 'foo',
        ]);
    }

    public function testToArray(): void
    {
        $event = ComponentConfigurationPurgedEvent::fromArray(self::EVENT_MESSAGE_DATA['data']);

        self::assertSame([
            'name' => 'storage.componentConfigurationPurged',
            'uuid' => 'uuid-123',
            'idProject' => 123,
            'idAccessToken' => 456,
            'accessTokenName' => 'access-token-name',
            'objectId' => 'object-id-123',
            'objectType' => 'configuration',
            'objectName' => 'My Kafka Data Destination',
            'message' => 'Component configuration purged',
            'params' => [
                'component' => 'keboola.wr-kafka',
                'configurationId' => '01k0vea53rhcw3anmg57w2k2aj',
                'name' => 'My Kafka Data Destination',
                'version' => 6,
            ],
            'idBranch' => 789,
        ], $event->toArray());
    }
}
