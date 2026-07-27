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

    public function getMetadata(): array
    {
        $metadata = $this->mapping['metadata'] ?? [];
        if (isset($this->mapping['description'])) {
            $metadata[self::DESCRIPTION_METADATA_KEY] = $this->mapping['description'];
        }
        return $metadata;
    }

    /**
     * Description of the column, stored in the native Storage description field. The configuration allows only
     * one of the two sources to be used at a time. An empty description is treated as no description, so that
     * it never clears a description stored in Storage.
     */
    public function getDescription(): ?string
    {
        if (isset($this->mapping['description'])) {
            return self::normalizeDescription($this->mapping['description']);
        }

        // metadata is a variableNode in the configuration, so it is not guaranteed to be an array
        $metadata = $this->mapping['metadata'] ?? [];
        if (is_array($metadata) && isset($metadata[self::DESCRIPTION_METADATA_KEY])) {
            return self::normalizeDescription($metadata[self::DESCRIPTION_METADATA_KEY]);
        }

        return null;
    }

    private static function normalizeDescription(mixed $description): ?string
    {
        if (!is_scalar($description)) {
            return null;
        }

        $description = (string) $description;

        return $description !== '' ? $description : null;
    }
}
