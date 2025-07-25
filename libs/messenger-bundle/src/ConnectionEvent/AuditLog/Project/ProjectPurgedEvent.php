<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\BaseAuditLogEvent;

class ProjectPurgedEvent extends BaseAuditLogEvent
{
    public const NAME = 'auditLog.project.purged';

    private string $projectId;
    private string $projectName;

    public static function fromArray(array $data): static
    {
        $event = parent::fromArray($data);

        $event->projectId = (string) $data['context']['project']['id'];
        $event->projectName = $data['context']['project']['name'];

        return $event;
    }

    public function toArray(): array
    {
        return parent::toArray() + [
            'context' => [
                'project' => [
                    'id' => $this->projectId,
                    'name' => $this->projectName,
                ],
            ],
        ];
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
