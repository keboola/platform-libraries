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

class TableDataModifier
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
    ) {
    }

    public function updateTableData(MappingFromProcessedConfiguration $source, MappingDestination $destination)
    {
        $config = $source->getMapping();
        if (!empty($config['delete_where_column'])) {
            // Delete rows
            $deleteOptions = [
                'whereColumn' => $config['delete_where_column'],
                'whereOperator' => $config['delete_where_operator'],
                'whereValues' => $config['delete_where_values'],
            ];
            $this->clientWrapper->getTableAndFileStorageClient()->deleteTableRows(
                $destination->getTableId(),
                $deleteOptions,
            );
        }

    }
}
