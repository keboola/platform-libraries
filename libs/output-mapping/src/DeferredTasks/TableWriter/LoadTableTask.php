<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\StorageApi\Client;

class LoadTableTask extends AbstractLoadTableTask
{
    protected function queueStorageJob(Client $client): string
    {
        // https://keboolaglobal.slack.com/archives/C05BK5V8N1Z/p1686822278887999?thread_ts=1686821168.533139&cid=C05BK5V8N1Z
        if (isset($this->options['columns']) && $this->options['columns'] === []) {
            unset($this->options['columns']);
        }
        return (string) $client->queueTableImport($this->destination->getTableId(), $this->options);
    }
}
