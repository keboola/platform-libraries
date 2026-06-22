<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationSchemaColumn
{
    private const DESCRIPTION_METADATA_KEY = 'KBC.description';

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

    /**
     * Column metadata without the description. Description is applied through the table-definition
     * endpoint (see TableDefinitionDescription), not stored as KBC.description metadata.
     */
    public function getMetadata(): array
    {
        $metadata = $this->mapping['metadata'] ?? [];
        unset($metadata[self::DESCRIPTION_METADATA_KEY]);
        return $metadata;
    }

    public function getDescription(): ?string
    {
        if (isset($this->mapping['description'])) {
            return $this->mapping['description'];
        }
        return $this->mapping['metadata'][self::DESCRIPTION_METADATA_KEY] ?? null;
    }
}
