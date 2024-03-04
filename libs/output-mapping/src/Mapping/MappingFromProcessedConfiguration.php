<?php

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MappingFromProcessedConfiguration
{
    private string $delimiter;
    private array $columns;

    /** @var MappingDestination  */
    private MappingDestination $destination;
    private MappingFromRawConfigurationAndPhysicalDataWithManifest $source;
    private array $mapping;

    public function __construct(
        array $mapping,
        MappingFromRawConfigurationAndPhysicalDataWithManifest $source,
    ) {
        // TODO validate mapping ?
        $this->mapping = $mapping;
        $this->source = $source;
        $this->destination = $this->mapping['destination'];
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

    /**
     * @deprecated
     * @TODO mapping je interni datova struktura, nesmi bejt public, protoze tohle je value object
     */
    public function getMapping(): array
    {
        return $this->mapping;
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
        return $this->mapping['column_metadata'];
    }

    public function getPathName(): string
    {
        return $this->source->getPathname();
    }

    public function hasDistributionKey(): bool
    {
        return !empty($this->mapping['distribution_key']);
    }

    public function getDistributionKey(): array
    {
        if (!$this->hasDistributionKey()) {
            throw new InvalidConfigurationException('Distribution key is not set.');
        }
        return $this->mapping['distribution_key'];
    }

    public function hasMetadata(): bool
    {
        return !empty($this->mapping['metadata']);
    }

    public function getMetadata(): array
    {
        return $this->mapping['metadata'];
    }

}
