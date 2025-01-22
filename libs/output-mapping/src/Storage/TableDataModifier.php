<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;

class TableDataModifier
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
    ) {
    }

    public function updateTableData(MappingFromProcessedConfiguration $source, MappingDestination $destination): void
    {
        $tableId = $destination->getTableId();
        $deleteOptionsList = [];

        if ($source->getDeleteWhereColumn() !== null) {
            $deleteOptionsList[] = DeleteTableRowsOptionsFactory::createFromLegacyDeleteWhereColumn(
                $source->getDeleteWhereColumn(),
                $source->getDeleteWhereOperator(),
                $source->getDeleteWhereValues(),
            );
        }

        foreach ($deleteOptionsList as $deleteOptions) {
            try {
                $this->clientWrapper->getTableAndFileStorageClient()->deleteTableRows(
                    $tableId,
                    $deleteOptions,
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot delete rows from table "%s" in Storage: %s',
                        $tableId,
                        $e->getMessage(),
                    ),
                    $e->getCode(),
                    $e,
                );
            }
        }
    }
}
