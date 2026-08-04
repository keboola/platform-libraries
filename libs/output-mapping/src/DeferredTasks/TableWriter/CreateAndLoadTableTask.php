<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\StorageApi\Client;

class CreateAndLoadTableTask extends AbstractLoadTableTask
{
    protected function queueStorageJob(Client $client): string
    {
        $options = $this->options;
        $options['name'] = $this->destination->getTableName();

        return (string) $client->queueTableCreate($this->destination->getBucketId(), $options);
    }
}
