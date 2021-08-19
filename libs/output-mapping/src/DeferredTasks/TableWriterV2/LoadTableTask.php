<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriterV2;

use Keboola\StorageApi\Client;

class LoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client)
    {
        $this->storageJobId = $client->queueTableImport($this->destination->getTableId(), $this->options);
    }
}
