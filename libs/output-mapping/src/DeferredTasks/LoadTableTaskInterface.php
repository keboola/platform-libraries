<?php

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

interface LoadTableTaskInterface
{
    public function startImport(Client $client);

    public function applyMetadata(Metadata $metadataApiClient);

    /**
     * @return string
     */
    public function getDestinationTableName();

    /**
     * @return null|string
     */
    public function getStorageJobId();
}
