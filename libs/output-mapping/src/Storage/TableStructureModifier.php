<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TableColumnsHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TableStructureModifier
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function updateTableStructure(
        BucketInfo $destinationBucket,
        TableInfo $destinationTableInfo,
        MappingFromProcessedConfiguration $source,
        MappingDestination $destination,
    ): void {
        TableColumnsHelper::addMissingColumns(
            $this->clientWrapper->getTableAndFileStorageClient(),
            $destinationTableInfo->asArray(),
            $source->getMapping(),
            $destinationBucket->backend,
        );

        if (PrimaryKeyHelper::modifyPrimaryKeyDecider($this->logger, $destinationTableInfo->asArray(), $source->getMapping())) {
            PrimaryKeyHelper::modifyPrimaryKey(
                $this->logger,
                $this->clientWrapper->getTableAndFileStorageClient(),
                $destination->getTableId(),
                $destinationTableInfo->primaryKey,
                $source->getPrimaryKey(),
            );
        }
    }
}
