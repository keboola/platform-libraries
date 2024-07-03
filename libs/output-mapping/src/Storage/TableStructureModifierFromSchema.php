<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchemaColumn;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TableStructureModifierFromSchema
{
    public function __construct(
        readonly ClientWrapper $clientWrapper,
        readonly LoggerInterface $logger,
    ) {
    }

    public function updateTableStructure(BucketInfo $bucket, TableInfo $table, TableChangesStore $changesStore): void
    {
        if ($changesStore->hasMissingColumns()) {
            $this->addColumns($table->getId(), $bucket->backend, $changesStore->getMissingColumns());
        }
    }

    private function addColumns(string $tableId, string $backend, array $missingColumns): void
    {
        $columnsAdded = [];
        foreach ($missingColumns as $missingColumn) {
            $columnData = new TableDefinitionFromSchemaColumn($missingColumn, $backend);

            $requestData = $columnData->getRequestData();
            try {
                $this->clientWrapper->getBranchClient()->addTableColumn(
                    $tableId,
                    $requestData['name'],
                    $requestData['definition'],
                    $requestData['basetype'],
                );
                $columnsAdded[] = $requestData['name'];
            } catch (ClientException $e) {
                // remove added columns
                foreach ($columnsAdded as $item) {
                    $this->clientWrapper->getBranchClient()->deleteTableColumn($tableId, $item);
                }
                throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }
}
