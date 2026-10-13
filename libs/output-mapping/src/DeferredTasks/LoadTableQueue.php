<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Storage\TableDescriptionModifier;
use Keboola\OutputMapping\Storage\TableInfo as StorageTableInfo;
use Keboola\OutputMapping\Table\Result;
use Keboola\OutputMapping\Table\Result\Metrics;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class LoadTableQueue
{
    private ClientWrapper $clientWrapper;
    private LoggerInterface $logger;

    /** @var DeferredTaskInterface[] */
    private array $tasks;

    /** @var array<string, TableDescription> table id => descriptions of a table created by this run */
    private array $createdTableDescriptions;

    private Result $tableResult;

    /**
     * @param DeferredTaskInterface[] $tasks
     * @param array<string, TableDescription> $createdTableDescriptions
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        array $tasks,
        array $createdTableDescriptions = [],
    ) {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->tasks = $tasks;
        $this->createdTableDescriptions = $createdTableDescriptions;
        $this->tableResult = new Result();
    }

    public function start(): void
    {
        foreach ($this->tasks as $task) {
            $task->start($this->clientWrapper);
        }
    }

    /**
     * @return string[] ids of the Storage jobs waited for
     */
    public function waitForAll(): array
    {
        $metadataApiClient = new Metadata($this->clientWrapper->getTableAndFileStorageClient());

        $jobIds = [];
        $errors = [];
        $jobResults = [];
        foreach ($this->tasks as $task) {
            foreach ($task->getStorageJobIds() as $jobId) {
                $jobIds[] = $jobId;
                /** @var array $jobResult */
                $jobResult = $this->clientWrapper->getBranchClient()->waitForJob($jobId);

                if (!$task instanceof LoadTableTaskInterface) {
                    // a job without a destination table carries nothing to describe, annotate or measure
                    if ($jobResult['status'] === 'error') {
                        $errors[] = $task->getFailedJobError($jobId, $jobResult);
                    }
                    continue;
                }

                if ($jobResult['status'] === 'error') {
                    $destinationTableName = $task->getDestinationTableName();
                    $errors[] = $task->getFailedJobError($jobId, $jobResult);
                    // The load failed, so there is no table to describe - it is about to be dropped below or it
                    // keeps the description it already had. Dropping the pending description here keeps it out of
                    // the "was not stored" warning, which is meant for descriptions lost by mistake.
                    unset($this->createdTableDescriptions[$destinationTableName]);

                    if (FailedLoadTableDecider::decideTableDelete($this->logger, $this->clientWrapper, $task)) {
                        $this->clientWrapper->getTableAndFileStorageClient()->dropTable(
                            $destinationTableName,
                            ['force' => true],
                        );
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
                            json_encode($extendedInfo),
                        );
                    }

                    switch ($jobResult['operationName']) {
                        case 'tableImport':
                            $tableData = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
                                $jobResult['tableId'],
                            );
                            $this->tableResult->addTable(new TableInfo($tableData));
                            $this->tableResult->addGenericVariable(
                                $jobResult['tableId'],
                                'importedRowsCount',
                                (int) ($jobResult['results']['importedRowsCount'] ?? 0),
                            );
                            $jobResults[] = $jobResult;
                            break;
                        case 'tableCreate':
                            $tableData = $this->clientWrapper->getTableAndFileStorageClient()->getTable(
                                $jobResult['results']['id'],
                            );
                            $this->tableResult->addTable(new TableInfo($tableData));
                            // Only a table created by the load job itself (CreateAndLoadTableTask) can have a
                            // pending description - every other table is created through a table definition,
                            // which carries the description in its create payload.
                            try {
                                $this->applyCreatedTableDescriptions($task, $tableData);
                            } catch (InvalidOutputException $e) {
                                $errors[] = $e->getMessage();
                            }
                            $jobResults[] = $jobResult;
                            break;
                    }
                }
            }
        }

        if ($this->createdTableDescriptions !== []) {
            // Must never happen - a description left here would be silently thrown away.
            $this->logger->warning(sprintf(
                'Description of table(s) "%s" was not stored.',
                implode('", "', array_keys($this->createdTableDescriptions)),
            ));
        }

        $this->tableResult->setMetrics(new Metrics($jobResults));

        if ($errors) {
            throw new InvalidOutputException(implode("\n", $errors));
        }
        return $jobIds;
    }

    /**
     * @param array<mixed> $tableData table detail as returned by Storage after a successful load
     */
    private function applyCreatedTableDescriptions(LoadTableTaskInterface $task, array $tableData): void
    {
        $tableId = $task->getDestinationTableName();
        $descriptions = $this->createdTableDescriptions[$tableId] ?? null;
        if ($descriptions === null) {
            return;
        }

        // Several sources may be mapped to the same destination table, but the descriptions of a table only
        // need to be stored once.
        unset($this->createdTableDescriptions[$tableId]);

        $descriptionModifier = new TableDescriptionModifier($this->clientWrapper, $this->logger);
        $descriptionModifier->updateDescriptions(new StorageTableInfo($tableData), $descriptions);
    }

    /**
     * Number of Storage jobs waitForAll() waits for. Only meaningful once the queue has been started.
     */
    public function getTaskCount(): int
    {
        return array_sum(array_map(
            fn(DeferredTaskInterface $task) => count($task->getStorageJobIds()),
            $this->tasks,
        ));
    }

    public function getTableResult(): Result
    {
        return $this->tableResult;
    }

    /**
     * @return LoadTableTaskInterface[]
     */
    public function getLoadTableTasks(): array
    {
        return array_values(array_filter(
            $this->tasks,
            fn(DeferredTaskInterface $task) => $task instanceof LoadTableTaskInterface,
        ));
    }

    public function loadCustomVariables(string $variablesFilePath): void
    {
        if (!is_file($variablesFilePath) || !is_readable($variablesFilePath)) {
            return;
        }
        $fileContent = file_get_contents($variablesFilePath);
        if ($fileContent === false) {
            return;
        }
        $content = json_decode($fileContent, true);
        if (!is_array($content) || !isset($content['variables']) || !is_array($content['variables'])) {
            $this->logger->warning(sprintf('Failed to parse result.json file "%s".', $variablesFilePath));
            return;
        }
        foreach ($content['variables'] as $key => $value) {
            if (is_scalar($value) || $value === null) {
                $this->tableResult->addCustomVariable((string) $key, $value);
            }
        }
    }
}
