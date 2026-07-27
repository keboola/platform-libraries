<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Writer\Helper\DescriptionHelper;

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

    /**
     * Column metadata without the description, which is stored in the native Storage description field
     * instead - see getDescription().
     */
    public function getMetadata(): array
    {
        return DescriptionHelper::removeDescriptionFromMetadataMap($this->mapping['metadata'] ?? []);
    }

    /**
     * Description of the column, stored in the native Storage description field. The configuration allows only
     * one of the two sources to be used at a time. An empty description is treated as no description, so that
     * it never clears a description stored in Storage.
     */
    public function getDescription(): ?string
    {
        if (isset($this->mapping['description'])) {
            return DescriptionHelper::normalizeDescription($this->mapping['description']);
        }

        // metadata is a variableNode in the configuration, so it is not guaranteed to be an array
        $metadata = $this->mapping['metadata'] ?? [];
        if (is_array($metadata) && isset($metadata[DescriptionHelper::DESCRIPTION_METADATA_KEY])) {
            return DescriptionHelper::normalizeDescription(
                $metadata[DescriptionHelper::DESCRIPTION_METADATA_KEY],
            );
        }

        return null;
    }
}
