<?php

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Configuration\Table\Configuration;
use Keboola\OutputMapping\Writer\Table\MappingDestination;

class MappingFromProcessedConfiguration
{
    private MappingDestination $destination;
    private MappingFromRawConfigurationAndPhysicalDataWithManifest $source;
    private array $mapping;

    public function __construct(
        array $mapping,
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
    ) {
        $this->destination = $mapping['destination'];
        unset($mapping['destination']);

        $this->mapping = (new Configuration())->parse([$mapping]);
        $this->source = $source;
    }

    public function isSliced(): bool
    {
        return $this->source->isSliced();
    }

    public function getSourceName(): string
    {
        return $this->source->getSourceName();
    }

    public function getDestination(): MappingDestination
    {
        return $this->destination;
    }

    public function getDelimiter(): string
    {
        return $this->mapping['delimiter'];
    }

    public function getEnclosure(): string
    {
        return $this->mapping['enclosure'];
    }

    public function getDeleteWhereColumn(): ?string
    {
        return $this->mapping['delete_where_column'] ?? null;
    }

    public function getDeleteWhereValues(): array
    {
        return $this->mapping['delete_where_values'];
    }

    public function getDeleteWhereOperator(): string
    {
        return $this->mapping['delete_where_operator'];
    }

    public function getPrimaryKey(): array
    {
        return $this->mapping['primary_key'];
    }

    public function hasWriteAlways(): bool
    {
        return $this->mapping['write_always'] ?? false;
    }

    public function hasColumns(): bool
    {
        return !empty($this->mapping['columns']);
    }

    public function hasColumnMetadata(): bool
    {
        return !empty($this->mapping['column_metadata']);
    }

    public function isIncremental()
    {
        return $this->mapping['incremental'];
    }

    public function getColumns(): array
    {
        return $this->mapping['columns'];
    }

    public function getColumnMetadata(): array
    {
        return $this->mapping['column_metadata'] ?? [];
    }

    public function getPathName(): string
    {
        return $this->source->getPathname();
    }

    public function getTags(): array
    {
        return $this->mapping['tags'] ?? [];
    }

    public function hasDistributionKey(): bool
    {
        return !empty($this->mapping['distribution_key']);
    }

    public function getDistributionKey(): array
    {
        return $this->mapping['distribution_key'] ?? [];
    }

    public function hasMetadata(): bool
    {
        return !empty($this->mapping['metadata']);
    }

    public function getMetadata(): array
    {
        return $this->mapping['metadata'] ?? [];
    }

}
