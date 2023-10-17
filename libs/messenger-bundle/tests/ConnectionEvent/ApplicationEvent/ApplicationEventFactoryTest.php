<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\ApplicationEvent;

use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEvent;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;
use PHPUnit\Framework\TestCase;

class ApplicationEventFactoryTest extends TestCase
{
    public function testCreateEvent(): void
    {
        $factory = new ApplicationEventFactory();
        $event = $factory->createEventFromArray([
            'class' => 'Event_Application',
            'created' => '2023-10-17T10:29:33+02:00',
            'data' => [
                'name' => 'ext.keboola.keboola-buffer.',
                'objectId' => '',
                'idProject' => 18,
                'params' => [
                    'task' => 'file-import',
                ],
                'idEvent' => 20219944,
                'type' => 'info',
                'context' => [
                    'remoteAddr' => '10.11.2.62',
                    'httpReferer' => null,
                    'httpUserAgent' => 'keboola-buffer-worker',
                    'apiVersion' => 'v2',
                ],
            ],
        ]);

        self::assertInstanceOf(ApplicationEvent::class, $event);
        self::assertSame('ext.keboola.keboola-buffer.', $event->name);
        self::assertSame('', $event->objectId);
        self::assertSame(18, $event->idProject);
        self::assertSame(['task' => 'file-import'], $event->params);
        self::assertSame(20219944, $event->idEvent);
        self::assertSame('info', $event->type);
        self::assertSame([
            'remoteAddr' => '10.11.2.62',
            'httpReferer' => null,
            'httpUserAgent' => 'keboola-buffer-worker',
            'apiVersion' => 'v2',
        ], $event->context);
    }

    public function testMissingDataProperty(): void
    {
        $factory = new ApplicationEventFactory();

        $this->expectException(EventFactoryException::class);
        $this->expectExceptionMessage('Missing or invalid property "data"');

        $factory->createEventFromArray([]);
    }

    public function testErrorHandlingDuringEventCreation(): void
    {
        $factory = new ApplicationEventFactory();

        $this->expectException(EventFactoryException::class);
        $this->expectExceptionMessage(
            'Failed to create Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory ' .
            'from event data: Event is missing property "name"',
        );

        $factory->createEventFromArray([
            'data' => [],
        ]);
    }
}
