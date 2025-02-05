<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\Exception\OutputOperationException;

class SystemMetadata
{
    private array $systemMetadata;

    public const SYSTEM_KEY_COMPONENT_ID = 'componentId';
    public const SYSTEM_KEY_CONFIGURATION_ID = 'configurationId';
    public const SYSTEM_KEY_CONFIGURATION_ROW_ID = 'configurationRowId';
    public const SYSTEM_KEY_BRANCH_ID = 'branchId';
    public const SYSTEM_KEY_RUN_ID = 'runId';
    public const SYSTEM_METADATA_PROVIDER = 'system';
    public const KBC_LAST_UPDATED_BY_BRANCH_ID = 'KBC.lastUpdatedBy.branch.id';
    public const KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';
    public const KBC_LAST_UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
    public const KBC_LAST_UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
    public const KBC_CREATED_BY_BRANCH_ID = 'KBC.createdBy.branch.id';
    public const KBC_CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
    public const KBC_CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
    public const KBC_CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';

    public function __construct(array $systemMetadata)
    {
        $this->systemMetadata = $systemMetadata;
        if (is_null($this->getSystemMetadata(self::SYSTEM_KEY_COMPONENT_ID))) {
            throw new OutputOperationException('Component Id must be set');
        }
    }

    public function asArray(): array
    {
        return $this->systemMetadata;
    }

    public function getSystemTags(): array
    {
        $systemTags = [
            self::SYSTEM_KEY_COMPONENT_ID,
            self::SYSTEM_KEY_CONFIGURATION_ID,
            self::SYSTEM_KEY_CONFIGURATION_ROW_ID,
            self::SYSTEM_KEY_BRANCH_ID,
            self::SYSTEM_KEY_RUN_ID,
        ];

        return array_filter($this->systemMetadata, function ($key) use ($systemTags) {
            return in_array($key, $systemTags);
        }, ARRAY_FILTER_USE_KEY);
    }

    public function getSystemMetadata(string $nameOfMetadata): ?string
    {
        return $this->systemMetadata[$nameOfMetadata] ?? null;
    }

    public function getCreatedMetadata(): array
    {
        $metadata[] = [
            'key' => self::KBC_CREATED_BY_COMPONENT_ID,
            'value' => $this->systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_CONFIGURATION_ID,
                'value' => $this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_CONFIGURATION_ROW_ID,
                'value' => $this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($this->systemMetadata[self::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_BRANCH_ID,
                'value' => $this->systemMetadata[self::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    public function getUpdatedMetadata(): array
    {
        $metadata[] = [
            'key' => self::KBC_LAST_UPDATED_BY_COMPONENT_ID,
            'value' => $this->systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_CONFIGURATION_ID,
                'value' => $this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID,
                'value' => $this->systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($this->systemMetadata[self::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_BRANCH_ID,
                'value' => $this->systemMetadata[self::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }
}
