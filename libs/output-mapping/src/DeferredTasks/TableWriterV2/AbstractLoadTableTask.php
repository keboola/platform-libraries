<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriterV2;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Metadata;

abstract class AbstractLoadTableTask implements LoadTableTaskInterface
{
    /** @var MappingDestination */
    protected $destination;

    /** @var array */
    protected $options;

    /** @var null|string */
    protected $storageJobId;

    /** @var MetadataInterface[] */
    protected $metadata = [];

    public function __construct(MappingDestination $destination, array $options)
    {
        $this->destination = $destination;
        $this->options = $options;
    }

    public function addMetadata(MetadataInterface $metadataDefinition)
    {
        $this->metadata[] = $metadataDefinition;
    }

    public function applyMetadata(Metadata $metadataApiClient)
    {
        foreach ($this->metadata as $metadataDefinition) {
            $metadataDefinition->apply($metadataApiClient);
        }
    }

    /**
     * @return null|string
     */
    public function getStorageJobId()
    {
        return $this->storageJobId;
    }
}
