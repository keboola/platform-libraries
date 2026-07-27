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
 *
 * Tables created by the current run are always system-managed (that is the Storage default), so their
 * description is stored without asking.
 */
class TableDescriptionModifier
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * Stores descriptions on a table which already existed before this run, unless its description is
     * managed by the user.
     */
    public function updateExistingTableDescriptions(TableInfo $tableInfo, TableDescription $descriptions): void
    {
        if (!$tableInfo->isDescriptionSystemManaged()) {
            $this->logger->info(sprintf(
                'Description of table "%s" is managed by the user, keeping the current value.',
                $tableInfo->getId(),
            ));
            return;
        }

        $this->applyDescriptions($descriptions, $tableInfo->getColumns());
    }

    /**
     * Stores descriptions on a table created by the current run.
     *
     * @param string[]|null $tableColumns columns of the created table, null when the column list is unknown
     */
    public function setCreatedTableDescriptions(TableDescription $descriptions, ?array $tableColumns): void
    {
        $this->applyDescriptions($descriptions, $tableColumns);
    }

    /**
     * @param string[]|null $tableColumns columns existing in the table, null disables the check
     */
    private function applyDescriptions(TableDescription $descriptions, ?array $tableColumns): void
    {
        $tableDefinitionUpdate = [];

        if ($descriptions->getTableDescription() !== null) {
            $tableDefinitionUpdate['description'] = $descriptions->getTableDescription();
        }

        $columns = [];
        $missingColumns = [];
        foreach ($descriptions->getColumnDescriptions() as $columnName => $columnDescription) {
            if ($tableColumns !== null && !in_array($columnName, $tableColumns, true)) {
                $missingColumns[] = $columnName;
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
                $descriptions->getTableId(),
            ));
        }

        if ($columns) {
            $tableDefinitionUpdate['columns'] = $columns;
        }

        if (!$tableDefinitionUpdate) {
            return;
        }

        try {
            $this->clientWrapper->getTableAndFileStorageClient()->updateTableDefinition(
                $descriptions->getTableId(),
                $tableDefinitionUpdate,
            );
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Cannot update description of table "%s": %s',
                    $descriptions->getTableId(),
                    $e->getMessage(),
                ),
                $e->getCode(),
                $e,
            );
        }
    }
}
