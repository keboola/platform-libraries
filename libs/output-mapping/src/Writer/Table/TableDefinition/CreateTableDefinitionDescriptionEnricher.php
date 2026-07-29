<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Storage\TableDescription;
use Psr\Log\LoggerInterface;

/**
 * Adds the table and column descriptions to the payload of POST /buckets/{id}/tables-definition, so that a
 * table created by output mapping carries its description right away and no extra (blocking) table-definition
 * update job is needed after the load.
 *
 * Beware of the two different shapes: the create endpoint nests the column description in
 * `columns[].definition.description`, while the update endpoint takes a flat `columns[].description`
 * (see TableDescriptionModifier). A `definition` containing only a description keeps the table non-typed,
 * so it is safe to add one to a column which has no `definition` at all.
 */
class CreateTableDefinitionDescriptionEnricher
{
    public function __construct(
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param array<mixed> $requestData payload as returned by TableDefinitionInterface::getRequestData()
     * @return array<mixed>
     */
    public function enrich(array $requestData, TableDescription $descriptions): array
    {
        if ($descriptions->isEmpty()) {
            return $requestData;
        }

        $tableDescription = $descriptions->getTableDescription();
        if ($tableDescription !== null) {
            $requestData['description'] = $tableDescription;
        }

        $columnDescriptions = $descriptions->getColumnDescriptions();
        if ($columnDescriptions === []) {
            return $requestData;
        }

        $columns = $requestData['columns'] ?? [];
        if (!is_array($columns)) {
            return $requestData;
        }

        $enrichedColumns = [];
        $knownColumns = [];
        foreach ($columns as $index => $column) {
            $columnName = is_array($column) ? ($column['name'] ?? null) : null;
            if (is_string($columnName) && isset($columnDescriptions[$columnName])) {
                $knownColumns[] = $columnName;

                $definition = $column['definition'] ?? [];
                if (!is_array($definition)) {
                    $definition = [];
                }
                $definition['description'] = $columnDescriptions[$columnName];
                $column['definition'] = $definition;
            }
            $enrichedColumns[$index] = $column;
        }

        // A description of a column which is not part of the payload must never append a new column entry -
        // that would create a column the data does not have.
        $missingColumns = array_diff(array_keys($columnDescriptions), $knownColumns);
        if ($missingColumns) {
            $this->logger->warning(sprintf(
                'Cannot store description of column(s) "%s" of table "%s", the column(s) do not exist.',
                implode('", "', $missingColumns),
                $descriptions->getTableId(),
            ));
        }

        $requestData['columns'] = $enrichedColumns;

        return $requestData;
    }
}
