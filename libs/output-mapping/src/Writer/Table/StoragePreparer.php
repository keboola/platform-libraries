<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Storage\BucketCreator;
use Keboola\OutputMapping\Storage\TableDataModifier;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\Storage\TableStructureModifier;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class StoragePreparer
{

    /**
     * @param ClientWrapper $clientWrapper
     * @param LoggerInterface $logger
     */
    public function __construct(readonly private ClientWrapper $clientWrapper, readonly private LoggerInterface $logger)
    {
    }

    public function prepareStorageBucketAndTable(
        MappingFromProcessedConfiguration $processedSource,
        SystemMetadata $systemMetadata,
    ): MappingStorageSources {
        $bucketCreator = new BucketCreator($this->clientWrapper);
        $destinationBucket = $bucketCreator->ensureDestinationBucket($processedSource->getDestination(), $systemMetadata);

        $destinationTableInfo = $this->getDestinationTableInfoIfExists($processedSource->getDestination()->getTableId());
        if ($destinationTableInfo !== null) {
            $tableStructureModifier = new TableStructureModifier($this->clientWrapper, $this->logger);
            $tableStructureModifier->updateTableStructure(
                $destinationBucket,
                $destinationTableInfo,
                $processedSource,
                $processedSource->getDestination(),
            );

            $tableDataModifier = new TableDataModifier($this->clientWrapper);
            $tableDataModifier->updateTableData(
                $processedSource,
                $processedSource->getDestination(),
            );
        }

        return new MappingStorageSources($destinationBucket, $destinationTableInfo);
    }

    private function getDestinationTableInfoIfExists(string $tableId): ?TableInfo
    {
        try {
            return new TableInfo($this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId));
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        return null;
    }
}