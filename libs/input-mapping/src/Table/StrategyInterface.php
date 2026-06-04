<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\TableLoadQueueInterface;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

interface StrategyInterface
{
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        StagingInterface $dataStorage,
        FileStagingInterface $metadataStorage,
        InputTableStateList $tablesState,
        string $destination,
        FileFormat $format,
    );

    /**
     * Convenience composition of prepareAndExecuteTableLoads() + waitForTableLoadCompletion()
     *
     * @param RewrittenInputTableOptions[] $tables
     */
    public function downloadTables(array $tables, bool $preserve): Result;

    /**
     * Phase 1 (start): plan the loads and submit the Storage jobs, return a handle for later completion
     *
     * @param RewrittenInputTableOptions[] $tables
     */
    public function prepareAndExecuteTableLoads(array $tables, bool $preserve): TableLoadQueueInterface;

    /**
     * Phase 2 (finish): await the jobs, materialize data/manifests and build the Result
     */
    public function waitForTableLoadCompletion(TableLoadQueueInterface $queue): Result;
}
