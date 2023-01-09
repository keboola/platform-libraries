<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\StorageApi\Client;

class LoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client)
    {
        $this->storageJobId = $client->queueTableImport($this->destination->getTableId(), $this->options);
    }
}
