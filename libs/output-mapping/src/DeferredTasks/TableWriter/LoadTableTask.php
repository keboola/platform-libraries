<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\StorageApi\Client;

class LoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client): void
    {
        $this->storageJobId = (string) $client->queueTableImport($this->destination->getTableId(), $this->options);
    }
}
