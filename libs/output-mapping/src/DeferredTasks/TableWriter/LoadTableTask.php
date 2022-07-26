<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class LoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client)
    {
        try {
            $this->storageJobId = $client->queueTableImport($this->destination->getTableId(), $this->options);
        } catch (ClientException $exception) {
            if ($exception->getCode() === 403) {
                throw new OutputOperationException(
                    sprintf('%s [%s]', $exception->getMessage(), $this->destination->getTableId()),
                    $exception
                );
            }
        }
    }
}
