<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApiBranch\ClientWrapper;

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
