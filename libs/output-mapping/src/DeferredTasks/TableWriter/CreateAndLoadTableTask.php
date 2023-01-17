<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\StorageApi\Client;

class CreateAndLoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client): void
    {
        $options = $this->options;
        $options['name'] = $this->destination->getTableName();

        $this->storageJobId = (string) $client->queueTableCreate($this->destination->getBucketId(), $options);
    }
}
