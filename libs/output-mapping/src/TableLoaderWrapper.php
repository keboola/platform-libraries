<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolverNew;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TableLoaderWrapper
{
    protected ClientWrapper $clientWrapper;
    protected LoggerInterface $logger;

    protected StrategyFactory $strategyFactory;

    public function __construct(StrategyFactory $strategyFactory)
    {
        $this->clientWrapper = $strategyFactory->getClientWrapper();
        $this->logger = $strategyFactory->getLogger();
        $this->strategyFactory = $strategyFactory;
        $this->logger = $strategyFactory->getLogger();
    }

    public function uploadTables(
        string $sourcePathPrefix,
        array $configuration,
        array $systemMetadata,
        string $stagingStorageOutput,
        bool $createTypedTables,
        bool $isFailedJob,
    ): LoadTableQueue {
        // TODO: this will be moved to caller
        $systemMetadata = new SystemMetadata($systemMetadata);
        $configuration = new OutputMappingSettings(
            $configuration,
            $sourcePathPrefix,
            $this->clientWrapper->getToken(),
            $createTypedTables,
            $isFailedJob,
        );
        $tableLoader = new TableLoader($this->logger, $this->clientWrapper, $this->strategyFactory);
        return $tableLoader->uploadTables(
            $stagingStorageOutput,
            $configuration,
            $systemMetadata,
        );
    }
}
