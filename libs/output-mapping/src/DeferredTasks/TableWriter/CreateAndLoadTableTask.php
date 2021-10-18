<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\StorageApi\Client;

class CreateAndLoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client)
    {
        $options = $this->options;
        $options['name'] = $this->destination->getTableName();

        $this->storageJobId = $client->queueTableCreate($this->destination->getBucketId(), $options);
    }
}
