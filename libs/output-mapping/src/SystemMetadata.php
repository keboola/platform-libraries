<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\TableWriter;

class SystemMetadata
{
    private array $systemMetadata;

    public function __construct(array $systemMetadata)
    {
        $this->systemMetadata = $systemMetadata;
        if (is_null($this->getSystemMetadata(AbstractWriter::SYSTEM_KEY_COMPONENT_ID))) {
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
            AbstractWriter::SYSTEM_KEY_COMPONENT_ID,
            AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID,
            AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID,
            AbstractWriter::SYSTEM_KEY_BRANCH_ID,
            AbstractWriter::SYSTEM_KEY_RUN_ID,
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
            'key' => TableWriter::KBC_CREATED_BY_COMPONENT_ID,
            'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_CONFIGURATION_ID,
                'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_CONFIGURATION_ROW_ID,
                'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($this->systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_BRANCH_ID,
                'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    public function getUpdatedMetadata(): array
    {
        $metadata[] = [
            'key' => TableWriter::KBC_LAST_UPDATED_BY_COMPONENT_ID,
            'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_CONFIGURATION_ID,
                'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID,
                'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($this->systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_BRANCH_ID,
                'value' => $this->systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }
}
