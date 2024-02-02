<?php

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\TableWriter;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Finder\SplFileInfo;

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
        // TODO move validation somewhere else
        $this->destination = new MappingDestination($this->mapping['destination']);
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
     */
    public function getMapping(): array
    {
        return $this->mapping;
    }

    public function getPrimaryKey(): array
    {
        return $this->mapping['primary_key'];
    }

    public function getPathName(): string
    {
        return $this->source->getPathname();
    }
}
