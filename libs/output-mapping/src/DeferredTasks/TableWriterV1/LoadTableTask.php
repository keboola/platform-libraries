<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriterV1;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

class LoadTableTask implements LoadTableTaskInterface
{
    /** @var string */
    private $destination;

    /** @var array */
    private $options;

    /** @var MetadataInterface[] */
    private $metadataDefinitions = [];

    /** @var null|string */
    private $storageJobId;

    public function __construct($destination, array $options)
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
        $this->storageJobId = $client->queueTableImport($this->destination, $this->options);
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
