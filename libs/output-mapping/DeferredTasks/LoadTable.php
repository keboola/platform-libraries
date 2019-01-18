<?php

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\StorageApi\Client;

class LoadTable
{
    /**
     * @var string
     */
    private $destination;

    /**
     * @var array
     */
    private $options;

    /**
     * @var MetadataDefinition[]
     */
    private $metadataDefinitions;

    /**
     * @var string
     */
    private $storageJobId;

    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client, $destination, array $options)
    {
        $this->client = $client;
        $this->destination = $destination;
        $this->options = $options;
    }

    public function startImport()
    {
        $this->storageJobId = $this->client->queueTableImport($this->destination, $this->options);
        return $this->storageJobId;
    }

    public function getStorageJobId()
    {
        return $this->storageJobId;
    }

    public function setMetadata()
    {
        foreach ($this->metadataDefinitions as $metadataDefinition) {
            $metadataDefinition->set();
        }
    }

    public function addMetadata(MetadataDefinition $metadataDefinition)
    {
        $this->metadataDefinitions[] = $metadataDefinition;
    }
}
