<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;

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
}
