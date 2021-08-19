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

    /** @var Client */
    private $client;

    public function __construct(Client $client, MappingDestination $destination, array $options)
    {
        $this->client = $client;
        $this->destination = $destination;
        $this->options = $options;
    }

    public function addMetadata(MetadataInterface $metadataDefinition)
    {
        $this->metadataDefinitions[] = $metadataDefinition;
    }

    public function startImport()
    {
        $this->storageJobId = $this->client->queueTableImport($this->destination->getTableId(), $this->options);
    }

    public function applyMetadata()
    {
        $metadataApiClient = new Metadata($this->client);

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
