<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\BaseAuditLogEvent;

class OrganizationDeletedEvent extends BaseAuditLogEvent
{
    public const NAME = 'auditLog.organization.deleted';

    private string $organizationId;
    private string $organizationName;

    public static function fromArray(array $data): static
    {
        $event = parent::fromArray($data);

        $event->organizationId = (string) $data['context']['organization']['id'];
        $event->organizationName = $data['context']['organization']['name'];

        return $event;
    }

    public function toArray(): array
    {
        return parent::toArray() + [
            'context' => [
                'organization' => [
                    'id' => $this->organizationId,
                    'name' => $this->organizationName,
                ],
            ],
        ];
    }

    public function getOrganizationId(): string
    {
        return $this->organizationId;
    }

    public function getOrganizationName(): string
    {
        return $this->organizationName;
    }
}
