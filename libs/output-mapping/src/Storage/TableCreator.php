<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;

class TableCreator
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
    ) {
    }

    public function createTableDefinition(
        string $bucketId,
        TableDefinitionInterface $tableDefinition,
    ): string {
        try {
            return $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
                $bucketId,
                $tableDefinition->getRequestData(),
            );
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Cannot create table "%s" definition in Storage API: %s',
                    $tableDefinition->getTableName(),
                    json_encode((array) $e->getContextParams()),
                ),
                $e->getCode(),
                $e,
            );
        }
    }

    public function createTable(
        string $bucketId,
        string $tableName,
        array $columns,
        array $loadOptions,
    ): string {
        $tmp = new Temp();

        $headerCsvFile = new CsvFile($tmp->createFile($tableName.'.header.csv')->getPathname());
        $headerCsvFile->writeRow($columns);

        try {
            return $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
                $bucketId,
                $tableName,
                $headerCsvFile,
                $loadOptions,
            );
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Cannot create table "%s" in Storage API: %s',
                    $tableName,
                    json_encode((array) $e->getContextParams()),
                ),
                $e->getCode(),
                $e,
            );
        }
    }
}
