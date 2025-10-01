<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\AuditLog;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Admin\AdminDeletedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\GenericAuditLogEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization\OrganizationDeletedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization\ProjectCreatedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectDeletedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectFeatureAddedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectFeatureRemovedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectPurgedEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectUndeletedEvent;
use Keboola\MessengerBundle\ConnectionEvent\EventFactoryInterface;
use Keboola\MessengerBundle\ConnectionEvent\EventInterface;
use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;
use Throwable;

class AuditEventFactory implements EventFactoryInterface
{
    /** @var array<string, class-string<EventInterface>> */
    private const EVENTS = [
        OrganizationDeletedEvent::NAME => OrganizationDeletedEvent::class,
        ProjectCreatedEvent::NAME => ProjectCreatedEvent::class,
        ProjectDeletedEvent::NAME => ProjectDeletedEvent::class,
        ProjectFeatureAddedEvent::NAME => ProjectFeatureAddedEvent::class,
        ProjectFeatureRemovedEvent::NAME => ProjectFeatureRemovedEvent::class,
        ProjectPurgedEvent::NAME => ProjectPurgedEvent::class,
        ProjectUndeletedEvent::NAME => ProjectUndeletedEvent::class,
        AdminDeletedEvent::NAME => AdminDeletedEvent::class,
    ];

    public function createEventFromArray(array $data): EventInterface
    {
        $eventData = $data['data'] ?? null;
        if (!is_array($eventData)) {
            throw new EventFactoryException('Missing or invalid property "data"');
        }

        $eventName = $eventData['operation'] ?? null;
        if ($eventName === null) {
            throw new EventFactoryException('Missing property "data.operation"');
        }

        $eventClass = self::EVENTS[$eventName] ?? null;
        if ($eventClass === null) {
            $eventClass = GenericAuditLogEvent::class;
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
