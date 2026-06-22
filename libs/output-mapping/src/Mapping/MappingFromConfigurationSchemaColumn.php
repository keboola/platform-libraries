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
     * Column metadata without the description. The description is handled separately (getDescription()):
     * for freshly created tables it goes into the table definition, otherwise as KBC.description metadata.
     */
    public function getMetadata(): array
    {
        $metadata = $this->mapping['metadata'] ?? [];
        // metadata is a variableNode in configuration, so it is not guaranteed to be an array
        if (!is_array($metadata)) {
            return [];
        }
        unset($metadata[self::DESCRIPTION_METADATA_KEY]);
        return $metadata;
    }

    public function getDescription(): ?string
    {
        if (isset($this->mapping['description'])) {
            return $this->mapping['description'];
        }
        $metadata = $this->mapping['metadata'] ?? [];
        if (is_array($metadata) && isset($metadata[self::DESCRIPTION_METADATA_KEY])) {
            return $metadata[self::DESCRIPTION_METADATA_KEY];
        }
        return null;
    }
}
