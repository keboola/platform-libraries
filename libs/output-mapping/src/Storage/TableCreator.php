<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\TableDefinition\CreateTableDefinitionDescriptionEnricher;
use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TableCreator
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * The descriptions are part of the create payload, so a table created here already carries them and no
     * table-definition update job is needed after the load. The table is brand new, therefore its description
     * is always system-managed (Storage default) and there is nothing to diff against.
     */
    public function createTableDefinition(
        string $bucketId,
        TableDefinitionInterface $tableDefinition,
        ?TableDescription $descriptions = null,
    ): string {
        $requestData = $tableDefinition->getRequestData();
        if ($descriptions !== null) {
            $requestData = (new CreateTableDefinitionDescriptionEnricher($this->logger))
                ->enrich($requestData, $descriptions);
        }

        try {
            return $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
                $bucketId,
                $requestData,
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
