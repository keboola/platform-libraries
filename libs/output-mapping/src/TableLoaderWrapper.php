<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TableLoaderWrapper
{
    protected ClientWrapper $clientWrapper;
    protected LoggerInterface $logger;

    protected StrategyFactory $strategyFactory;
    private Metadata $metadataClient;
    private TableConfigurationResolver $tableConfigurationResolver;

    public function __construct(StrategyFactory $strategyFactory)
    {
        $this->clientWrapper = $strategyFactory->getClientWrapper();
        $this->logger = $strategyFactory->getLogger();
        $this->strategyFactory = $strategyFactory;

        $this->metadataClient = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $this->tableConfigurationResolver = new TableConfigurationResolver(
            $strategyFactory->getLogger(),
        );
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
            $createTypedTables
        );
        $tableLoader = new TableLoader($this->logger, $this->clientWrapper, $this->strategyFactory);
        return $tableLoader->uploadTables(
            $stagingStorageOutput,
            $configuration,
            $systemMetadata,
            $isFailedJob, // TODO: fix this, how about about isDebugJob ?
        );
    }
}
