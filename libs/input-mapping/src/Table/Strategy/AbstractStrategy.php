<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Helper\LogFormatter;
use Keboola\InputMapping\Helper\Timer;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Result;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\InputMapping\Table\StrategyInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategy implements StrategyInterface
{
    protected readonly ClientWrapper $clientWrapper; // @phpstan-ignore-line initialized in child classes
    protected readonly LoggerInterface $logger; // @phpstan-ignore-line initialized in child classes

    public function waitForTableLoadCompletion(TableLoadQueueInterface $queue): Result
    {
        $timer = Timer::start();
        $jobIds = $queue->getJobIds();
        $jobResults = $jobIds === [] ? [] : $this->awaitJobs($jobIds);

        // per-table "Fetched table ..." lines are emitted by the strategies, as each table finishes
        $this->materializeTableLoads($queue, $jobResults);

        $outputStateConfiguration = [];
        $result = new Result();
        $tables = $queue->getAllTables();
        foreach ($tables as $table) {
            $outputStateConfiguration[] = [
                'source' => $table->getSource(),
                'lastImportDate' => $table->getTableInfo()['lastImportDate'],
            ];
            $result->addTable(new TableInfo($table->getTableInfo()));
        }
        $result->setMetrics($jobResults);
        $result->setInputTableStateList(new InputTableStateList($outputStateConfiguration));
        $this->logger->info('All tables were fetched.');
        $this->logger->debug(sprintf(
            'All tables were fetched. %s tables in %s.',
            count($tables),
            LogFormatter::formatDuration($timer->getElapsedSeconds()),
        ));

        return $result;
    }

    /**
     * Storage jobs are polled sequentially by handleAsyncTasks(), so only the whole wait phase is
     * timed - a per-job duration would just reflect the poll order.
     *
     * @param array<int|string> $jobIds
     * @return array results of the finished Storage jobs, as returned by handleAsyncTasks()
     */
    private function awaitJobs(array $jobIds): array
    {
        $this->logger->debug(sprintf('Waiting for %s storage jobs to finish.', count($jobIds)));
        $timer = Timer::start();
        $jobResults = $this->getAwaitingClient()->handleAsyncTasks($jobIds);
        $this->logger->debug(sprintf(
            '%s storage jobs finished in %s.',
            count($jobIds),
            LogFormatter::formatDuration($timer->getElapsedSeconds()),
        ));

        return $jobResults;
    }

    /**
     * Emits the per-table completion line.
     *
     * `Fetched table <source>.` is a log contract - job logs are grepped for it - so it stays
     * byte-identical at info level. Timing, size and Storage job identifiers are diagnostics, so they go
     * out as a separate debug line that repeats the table id to stay readable on its own.
     */
    protected function logTableFetched(string $source, string $details = ''): void
    {
        $this->logger->info(sprintf('Fetched table %s.', $source));
        if ($details !== '') {
            $this->logger->debug(sprintf('Fetched table %s. %s', $source, $details));
        }
    }

    protected function logManifestsWritten(int $count, float $seconds): void
    {
        $this->logger->debug(sprintf(
            'Wrote %s table manifests in %s.',
            $count,
            LogFormatter::formatDuration($seconds),
        ));
    }

    /**
     * Strategy-specific completion: download/enrich data, write manifests and log each finished table
     *
     * @param array $jobResults results of the finished Storage jobs (untyped, as returned by handleAsyncTasks())
     */
    abstract protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void;

    protected function getAwaitingClient(): Client
    {
        return $this->clientWrapper->getBranchClient();
    }
}
