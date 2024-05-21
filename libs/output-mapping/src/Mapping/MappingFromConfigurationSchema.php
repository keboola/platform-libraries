<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationSchema
{
    public function __construct(private readonly array $mapping)
    {
    }

    public function getName(): string
    {
        return $this->mapping['name'];
    }

    public function getDataType(): ?MappingFromConfigurationSchemaDataType
    {
        if (!isset($this->mapping['data_type'])) {
            return null;
        }
        return new MappingFromConfigurationSchemaDataType($this->mapping['data_type']);
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

    public function getDescription(): string
    {
        return $this->mapping['description'] ?? '';
    }

    public function geMetadata(): array
    {
        return $this->mapping['metadata'] ?? [];
    }
}
