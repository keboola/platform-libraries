<?php

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;

class LoadTableQueue
{
    /** @var Client */
    private $client;

    /** @var LoadTableTaskInterface[] */
    private $loadTableTasks;

    /**
     * @param LoadTableTaskInterface[] $loadTableTasks
     */
    public function __construct(Client $client, array $loadTableTasks)
    {
        $this->client = $client;
        $this->loadTableTasks = $loadTableTasks;
    }

    public function start()
    {
        foreach ($this->loadTableTasks as $loadTableTask) {
            $loadTableTask->startImport($this->client);
        }
    }

    public function waitForAll()
    {
        $metadataApiClient = new Metadata($this->client);

        $jobIds = [];
        $errors = [];
        foreach ($this->loadTableTasks as $task) {
            $jobIds[] = $task->getStorageJobId();
            $jobResult = $this->client->waitForJob($task->getStorageJobId());
            if ($jobResult['status'] === 'error') {
                $errors[] = sprintf('Failed to load table "%s": %s', $task->getDestinationTableName(), $jobResult['error']['message']);
            } else {
                $task->applyMetadata($metadataApiClient);
            }
        }
        if ($errors) {
            throw new InvalidOutputException(implode("\n", $errors));
        }
        return $jobIds;
    }

    public function getTaskCount()
    {
        return count($this->loadTableTasks);
    }
}
