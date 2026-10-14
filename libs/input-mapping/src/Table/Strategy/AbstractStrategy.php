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
        $jobResults = [];
        if ($jobIds !== []) {
            $jobResults = $this->getAwaitingClient()->handleAsyncTasks($jobIds);
            $this->logger->info(sprintf(
                '%s storage jobs finished in %.2f s.',
                count($jobIds),
                $timer->getElapsedSeconds(),
            ));
        }

        $this->materializeTableLoads($queue, $jobResults);

        $outputStateConfiguration = [];
        $result = new Result();
        foreach ($queue->getAllTables() as $table) {
            $outputStateConfiguration[] = [
                'source' => $table->getSource(),
                'lastImportDate' => $table->getTableInfo()['lastImportDate'],
            ];
            $this->logger->info('Fetched table ' . $table->getSource() . '.');
            $result->addTable(new TableInfo($table->getTableInfo()));
        }
        $result->setMetrics($jobResults);
        $result->setInputTableStateList(new InputTableStateList($outputStateConfiguration));
        $this->logger->info(sprintf('All tables were fetched. Took %.2f s.', $timer->getElapsedSeconds()));

        return $result;
    }

    /**
     * Strategy-specific completion: download/enrich data and write manifests
     *
     * @param array $jobResults results of the finished Storage jobs (untyped, as returned by handleAsyncTasks())
     */
    abstract protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void;

    protected function getAwaitingClient(): Client
    {
        return $this->clientWrapper->getBranchClient();
    }
}
