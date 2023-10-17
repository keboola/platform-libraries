<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent;

use Keboola\MessengerBundle\ConnectionEvent\EventFactoryInterface;
use Keboola\MessengerBundle\ConnectionEvent\EventInterface;
use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;
use Throwable;

class ApplicationEventFactory implements EventFactoryInterface
{
    public function createEventFromArray(array $data): EventInterface
    {
        $eventData = $data['data'] ?? null;
        if (!is_array($eventData)) {
            throw new EventFactoryException('Missing or invalid property "data"');
        }

        try {
            return ApplicationEvent::fromArray($eventData);
        } catch (Throwable $e) {
            throw new EventFactoryException(
                sprintf('Failed to create %s from event data: %s', ApplicationEventFactory::class, $e->getMessage()),
                0,
                $e,
            );
        }
    }
}
