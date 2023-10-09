<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\BaseAuditLogEvent;

class ProjectFeatureAddedEvent extends BaseAuditLogEvent
{
    public const NAME = 'auditLog.project.featureAdded';

    private string $projectId;
    private string $projectName;

    private string $featureId;
    private string $featureName;
    private string $featureType;

    public static function fromArray(array $data): static
    {
        $event = parent::fromArray($data);

        $event->projectId = (string) $data['context']['project']['id'];
        $event->projectName = $data['context']['project']['name'];

        $event->featureId = (string) $data['context']['feature']['id'];
        $event->featureName = $data['context']['feature']['name'];
        $event->featureType = $data['context']['feature']['type'];

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

                'feature' => [
                    'id' => $this->featureId,
                    'name' => $this->featureName,
                    'type' => $this->featureType,
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

    public function getFeatureId(): string
    {
        return $this->featureId;
    }

    public function getFeatureName(): string
    {
        return $this->featureName;
    }

    public function getFeatureType(): string
    {
        return $this->featureType;
    }
}
