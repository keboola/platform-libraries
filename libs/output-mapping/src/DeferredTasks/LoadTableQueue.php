<?php

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Table\Result;
use Keboola\OutputMapping\Table\Result\Metrics;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;

class LoadTableQueue
{
    /** @var Client */
    private $client;

    /** @var LoadTableTaskInterface[] */
    private $loadTableTasks;

    /** @var Result */
    private $tableResult;

    /**
     * @param LoadTableTaskInterface[] $loadTableTasks
     */
    public function __construct(Client $client, array $loadTableTasks)
    {
        $this->client = $client;
        $this->loadTableTasks = $loadTableTasks;
        $this->tableResult = new Result();
    }

    public function start()
    {
        foreach ($this->loadTableTasks as $loadTableTask) {
            try {
                $loadTableTask->startImport($this->client);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf('%s [%s]', $e->getMessage(), $loadTableTask->getDestinationTableName()),
                    $e->getCode(),
                    $e
                );
            }
        }
    }

    public function waitForAll()
    {
        $metadataApiClient = new Metadata($this->client);

        $jobIds = [];
        $errors = [];
        $jobResults = [];
        foreach ($this->loadTableTasks as $task) {
            $jobId = $task->getStorageJobId();
            $jobIds[] = $jobId;
            $jobResult = $this->client->waitForJob($jobId);

            if ($jobResult['status'] === 'error') {
                $errors[] = sprintf('Failed to load table "%s": %s', $task->getDestinationTableName(), $jobResult['error']['message']);
            } else {
                $task->applyMetadata($metadataApiClient);

                switch ($jobResult['operationName']) {
                    case 'tableImport':
                        $this->tableResult->addTable(new TableInfo($this->client->getTable($jobResult['tableId'])));
                        $jobResults[] = $jobResult;
                        break;
                    case 'tableCreate':
                        $this->tableResult->addTable(new TableInfo($this->client->getTable($jobResult['results']['id'])));
                        $jobResults[] = $jobResult;
                        break;
                }
            }
        }

        $this->tableResult->setMetrics(new Metrics($jobResults));

        if ($errors) {
            throw new InvalidOutputException(implode("\n", $errors));
        }
        return $jobIds;
    }

    public function getTaskCount()
    {
        return count($this->loadTableTasks);
    }

    public function getTableResult()
    {
        return $this->tableResult;
    }
}
