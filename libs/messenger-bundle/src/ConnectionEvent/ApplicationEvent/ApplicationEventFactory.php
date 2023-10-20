<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent;

use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchCreatedEvent;
use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\Storage\DevBranchDeletedEvent;
use Keboola\MessengerBundle\ConnectionEvent\EventFactoryInterface;
use Keboola\MessengerBundle\ConnectionEvent\EventInterface;
use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;
use Throwable;

class ApplicationEventFactory implements EventFactoryInterface
{
    /** @var array<string, class-string<EventInterface>> */
    private const EVENTS = [
        DevBranchCreatedEvent::NAME => DevBranchCreatedEvent::class,
        DevBranchDeletedEvent::NAME => DevBranchDeletedEvent::class,
    ];

    public function createEventFromArray(array $data): EventInterface
    {
        $eventData = $data['data'] ?? null;
        if (!is_array($eventData)) {
            throw new EventFactoryException('Missing or invalid property "data"');
        }

        $eventName = $eventData['name'] ?? null;
        if ($eventName === null) {
            throw new EventFactoryException('Missing property "data.name"');
        }

        $eventClass = self::EVENTS[$eventName] ?? null;
        if ($eventClass === null) {
            $eventClass = GenericApplicationEvent::class;
        }

        try {
            return $eventClass::fromArray($eventData);
        } catch (Throwable $e) {
            throw new EventFactoryException(
                sprintf('Failed to create %s from event data: %s', $eventClass, $e->getMessage()),
                0,
                $e,
            );
        }
    }
}
