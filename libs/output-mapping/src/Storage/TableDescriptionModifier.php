<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

/**
 * Stores table and column descriptions in the native Storage description field through the table-definition
 * update endpoint (PUT /tables/{id}/definition).
 *
 * Whether the description may be stored is driven by the table-level `isDescriptionSystemManaged` flag
 * (AJDA-2946, DMD-1662):
 * - system-managed - output mapping stores/updates the table and column description,
 * - user-managed - output mapping discards the description it produced so that the value set by the user
 *   is never overwritten.
 */
class TableDescriptionModifier
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function updateDescriptions(TableInfo $tableInfo, TableDescription $descriptions): void
    {
        if (!$tableInfo->isDescriptionSystemManaged()) {
            $this->logger->info(sprintf(
                'Description of table "%s" is managed by the user, keeping the current value.',
                $tableInfo->getId(),
            ));
            return;
        }

        $tableDefinitionUpdate = $this->buildTableDefinitionUpdate($tableInfo, $descriptions);
        if (!$tableDefinitionUpdate) {
            // Nothing changed since the last run. Storage rejects a patch without any effective change with
            // "No table definition changes were provided." (400), so the call must be skipped entirely.
            return;
        }

        try {
            $this->clientWrapper->getTableAndFileStorageClient()->updateTableDefinition(
                $tableInfo->getId(),
                $tableDefinitionUpdate,
            );
        } catch (ClientException $e) {
            if ($e->getCode() < 400 || $e->getCode() >= 500) {
                // Only a 4xx is the caller's fault. A Storage outage and a connection error (which carries no
                // HTTP status, i.e. code 0) both stay a retryable application error, consistently with the
                // metadata path in LoadTableQueue.
                throw $e;
            }

            throw new InvalidOutputException(
                sprintf(
                    'Cannot update description of table "%s": %s',
                    $tableInfo->getId(),
                    $e->getMessage(),
                ),
                $e->getCode(),
                $e,
            );
        }
    }

    /**
     * @return array{description?: string, columns?: list<array{name: string, description: string}>}
     */
    private function buildTableDefinitionUpdate(TableInfo $tableInfo, TableDescription $descriptions): array
    {
        $tableDefinitionUpdate = [];

        $tableDescription = $descriptions->getTableDescription();
        if ($tableDescription !== null && $tableDescription !== $tableInfo->getDescription()) {
            $tableDefinitionUpdate['description'] = $tableDescription;
        }

        $tableColumns = $tableInfo->getColumns();
        $storedColumnDescriptions = $tableInfo->getColumnDescriptions();

        $columns = [];
        $missingColumns = [];
        foreach ($descriptions->getColumnDescriptions() as $columnName => $columnDescription) {
            if (!in_array($columnName, $tableColumns, true)) {
                $missingColumns[] = $columnName;
                continue;
            }
            if ($columnDescription === ($storedColumnDescriptions[$columnName] ?? null)) {
                continue;
            }
            $columns[] = [
                'name' => $columnName,
                'description' => $columnDescription,
            ];
        }

        if ($missingColumns) {
            $this->logger->warning(sprintf(
                'Cannot store description of column(s) "%s" of table "%s", the column(s) do not exist.',
                implode('", "', $missingColumns),
                $tableInfo->getId(),
            ));
        }

        if ($columns) {
            $tableDefinitionUpdate['columns'] = $columns;
        }

        return $tableDefinitionUpdate;
    }
}
