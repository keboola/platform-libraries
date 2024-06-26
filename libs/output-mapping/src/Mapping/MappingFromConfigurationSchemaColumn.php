<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationSchemaColumn
{
    public function __construct(private readonly array $mapping)
    {
    }

    public function getName(): string
    {
        return $this->mapping['name'];
    }

    public function getDataType(): ?MappingFromConfigurationSchemaColumnDataType
    {
        if (!isset($this->mapping['data_type'])) {
            return null;
        }
        return new MappingFromConfigurationSchemaColumnDataType($this->mapping['data_type']);
    }

    public function isNullable(): bool
    {
        return $this->mapping['nullable'] ?? true;
    }

    public function isPrimaryKey(): bool
    {
        return $this->mapping['primary_key'] ?? false;
    }

    public function isDistributionKey(): bool
    {
        return $this->mapping['distribution_key'] ?? false;
    }

    public function hasMetadata(): bool
    {
        return !empty($this->getMetadata());
    }

    public function getMetadata(): array
    {
        $metadata = $this->mapping['metadata'] ?? [];
        if (isset($this->mapping['description'])) {
            $metadata['KBC.description'] = $this->mapping['description'];
        }
        return $metadata;
    }
}
