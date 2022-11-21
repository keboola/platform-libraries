<?php

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class CreateAndLoadTableTask extends AbstractLoadTableTask
{
    public function startImport(Client $client)
    {
        $options = $this->options;
        $options['name'] = $this->destination->getTableName();
        try {
            $this->storageJobId = $client->queueTableCreate($this->destination->getBucketId(), $options);
        } catch (ClientException $exception) {
            if ($exception->getCode() === 403) {
                throw new OutputOperationException(
                    sprintf('%s [%s]', $exception->getMessage(), $this->destination->getBucketId()),
                    $exception
                );
            }

            throw $exception;
        }
    }
}
