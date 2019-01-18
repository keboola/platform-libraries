<?php

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Client;

class LoadTableQueue
{
    /**
     * @var LoadTable[]
     */
    private $loadTableTasks;

    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client, array $loadTableTasks)
    {
        $this->client = $client;
        $this->loadTableTasks = $loadTableTasks;
    }

    public function start()
    {
        foreach ($this->loadTableTasks as $loadTableTask) {
            $loadTableTask->startImport();
        }
    }

    public function waitForAll()
    {
        $jobIds = [];
        $errors = [];
        foreach ($this->loadTableTasks as $task) {
            $jobResult = $this->client->waitForJob($task->getStorageJobId());
            if ($jobResult['status'] == 'error') {
                $errors[] = $jobResult['error']['message'];
            }
            $task->setMetadata();
            $jobIds[] = $task->getStorageJobId();
        }
        if ($errors) {
            throw new InvalidOutputException('Failed to load tables: ' . implode(' ', $errors), null);
        }
        return $jobIds;
    }
}
