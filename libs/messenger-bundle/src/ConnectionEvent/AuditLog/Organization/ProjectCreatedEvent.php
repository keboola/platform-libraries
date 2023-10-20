<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\BaseAuditLogEvent;

class ProjectCreatedEvent extends BaseAuditLogEvent
{
    public const NAME = 'auditLog.organization.projectCreated';

    private string $organizationId;
    private string $organizationName;

    private string $projectId;
    private string $projectName;

    public static function fromArray(array $data): static
    {
        $event = parent::fromArray($data);

        $event->organizationId = (string) $data['context']['organization']['id'];
        $event->organizationName = $data['context']['organization']['name'];

        $event->projectId = (string) $data['context']['project']['id'];
        $event->projectName = $data['context']['project']['name'];

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
                'project' => [
                    'id' => $this->projectId,
                    'name' => $this->projectName,
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

    public function getProjectId(): string
    {
        return $this->projectId;
    }

    public function getProjectName(): string
    {
        return $this->projectName;
    }
}
