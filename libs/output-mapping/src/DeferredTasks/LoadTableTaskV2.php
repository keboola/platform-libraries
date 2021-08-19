<?php

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

class LoadTableTaskV2 implements LoadTableTaskInterface
{
    /** @var string */
    private $destination;

    /** @var array */
    private $options;

    /** @var MetadataInterface[] */
    private $metadataDefinitions = [];

    /** @var null|string */
    private $storageJobId;

    public function __construct(MappingDestination $destination, array $options)
    {
        $this->destination = $destination;
        $this->options = $options;
    }

    public function addMetadata(MetadataInterface $metadataDefinition)
    {
        $this->metadataDefinitions[] = $metadataDefinition;
    }

    public function startImport(Client $client)
    {
        $this->storageJobId = $client->queueTableImport($this->destination->getTableId(), $this->options);
    }

    public function applyMetadata(Metadata $metadataApiClient)
    {
        foreach ($this->metadataDefinitions as $metadataDefinition) {
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
