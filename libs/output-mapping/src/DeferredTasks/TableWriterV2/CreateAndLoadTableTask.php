<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriterV2;

use Keboola\OutputMapping\DeferredTasks\Metadata\MetadataInterface;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\AbstractLoadTableTask;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

class CreateAndLoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client)
    {
        $options = $this->options;
        $options['name'] = $this->destination->getTableName();

        $this->storageJobId = $client->queueTableCreate($this->destination->getBucketId(), $options);
    }
}
