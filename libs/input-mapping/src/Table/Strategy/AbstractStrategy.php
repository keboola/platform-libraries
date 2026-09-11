<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

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
        $this->logger->info(sprintf(
            'All tables were fetched. %s tables in %.2f s.',
            count($tables),
            $timer->getElapsedSeconds(),
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
        $this->logger->info(sprintf('Waiting for %s storage jobs to finish.', count($jobIds)));
        $timer = Timer::start();
        $jobResults = $this->getAwaitingClient()->handleAsyncTasks($jobIds);
        $this->logger->info(sprintf(
            '%s storage jobs finished in %.2f s.',
            count($jobIds),
            $timer->getElapsedSeconds(),
        ));

        return $jobResults;
    }

    /**
     * Emits the per-table completion line.
     *
     * `Fetched table <source>.` is a log contract - job logs are grepped for it - so the message always
     * starts with it; timing, size and Storage job identifiers are appended after it.
     */
    protected function logTableFetched(string $source, string $details = ''): void
    {
        $message = sprintf('Fetched table %s.', $source);
        if ($details !== '') {
            $message .= ' ' . $details;
        }
        $this->logger->info($message);
    }

    protected function logManifestsWritten(int $count, float $seconds): void
    {
        $this->logger->info(sprintf('Wrote %s table manifests in %.2f s.', $count, $seconds));
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
