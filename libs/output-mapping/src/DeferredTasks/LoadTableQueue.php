<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks;

use DateInterval;
use DateTimeImmutable;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Table\Result;
use Keboola\OutputMapping\Table\Result\Metrics;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Psr\Log\LoggerInterface;

class LoadTableQueue
{
    private Client $client;
    private LoggerInterface $logger;

    /** @var LoadTableTaskInterface[] */
    private array $loadTableTasks;
    private Result $tableResult;

    /**
     * @param LoadTableTaskInterface[] $loadTableTasks
     */
    public function __construct(Client $client, LoggerInterface $logger, array $loadTableTasks)
    {
        $this->client = $client;
        $this->logger = $logger;
        $this->loadTableTasks = $loadTableTasks;
        $this->tableResult = new Result();
    }

    public function start(): void
    {
        foreach ($this->loadTableTasks as $loadTableTask) {
            try {
                $loadTableTask->startImport($this->client);
            } catch (ClientException $e) {
                if ($e->getCode() < 500) {
                    throw new InvalidOutputException(
                        sprintf('%s [%s]', $e->getMessage(), $loadTableTask->getDestinationTableName()),
                        $e->getCode(),
                        $e
                    );
                }

                throw $e;
            }
        }
    }

    public function waitForAll(): array
    {
        $metadataApiClient = new Metadata($this->client);

        $jobIds = [];
        $errors = [];
        $jobResults = [];
        foreach ($this->loadTableTasks as $task) {
            $jobId = $task->getStorageJobId();
            $jobIds[] = $jobId;
            /** @var array $jobResult */
            $jobResult = $this->client->waitForJob($jobId);

            if ($jobResult['status'] === 'error') {
                $errors[] = sprintf(
                    'Failed to load table "%s": %s',
                    $task->getDestinationTableName(),
                    $jobResult['error']['message']
                );
                if (FailedLoadTableDecider::decideTableDelete($this->logger, $this->client, $task)) {
                    $this->client->dropTable($task->getDestinationTableName(), ['force' => true]);
                }
            } else {
                try {
                    $task->applyMetadata($metadataApiClient);
                } catch (ClientException $e) {
                    if ($e->getCode() >= 500) {
                        throw $e;
                    }
                    $extendedInfo = $e->getContextParams()['errors'] ?? [];
                    $errors[] = sprintf(
                        'Failed to update metadata for table "%s": %s (%s)',
                        $task->getDestinationTableName(),
                        $e->getMessage(),
                        json_encode($extendedInfo)
                    );
                }

                switch ($jobResult['operationName']) {
                    case 'tableImport':
                        $this->tableResult->addTable(new TableInfo($this->client->getTable($jobResult['tableId'])));
                        $jobResults[] = $jobResult;
                        break;
                    case 'tableCreate':
                        $this->tableResult->addTable(
                            new TableInfo($this->client->getTable($jobResult['results']['id']))
                        );
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

    public function getTaskCount(): int
    {
        return count($this->loadTableTasks);
    }

    public function getTableResult(): Result
    {
        return $this->tableResult;
    }
}
