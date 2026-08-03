<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Storage\TableDescription;
use Psr\Log\LoggerInterface;

/**
 * Renders the table and column descriptions into the payload of POST /buckets/{id}/tables-definition, so that
 * a table created by output mapping carries them right away and no extra (blocking) table-definition update
 * job is needed after the load.
 *
 * Beware of the two different shapes: the create endpoint nests the column description in
 * `columns[].definition.description`, while the update endpoint takes a flat `columns[].description`
 * (see TableDescriptionModifier). A `definition` containing only a description keeps the table non-typed,
 * so it is safe to add one to a column which has no `definition` at all.
 */
trait TableDefinitionDescriptionsTrait
{
    private ?TableDescription $descriptions = null;

    private ?LoggerInterface $descriptionsLogger = null;

    public function setDescriptions(TableDescription $descriptions, LoggerInterface $logger): void
    {
        $this->descriptions = $descriptions;
        $this->descriptionsLogger = $logger;
    }

    /**
     * Called by getRequestData() of the implementing definition, so that the payload it returns is always the
     * one really sent to Storage.
     *
     * @param array<mixed> $requestData
     * @return array<mixed>
     */
    private function withDescriptions(array $requestData): array
    {
        if ($this->descriptions === null || $this->descriptions->isEmpty()) {
            return $requestData;
        }

        $tableDescription = $this->descriptions->getTableDescription();
        if ($tableDescription !== null) {
            $requestData['description'] = $tableDescription;
        }

        $columnDescriptions = $this->descriptions->getColumnDescriptions();
        if ($columnDescriptions === []) {
            return $requestData;
        }

        $knownColumns = [];
        foreach ($requestData['columns'] as $index => $column) {
            $columnName = $column['name'] ?? null;
            if (!is_string($columnName) || !isset($columnDescriptions[$columnName])) {
                continue;
            }
            $knownColumns[] = $columnName;

            $definition = $column['definition'] ?? [];
            $definition['description'] = $columnDescriptions[$columnName];
            $requestData['columns'][$index]['definition'] = $definition;
        }

        // A description of a column which is not part of the payload must never append a new column entry -
        // that would create a column the data does not have.
        $missingColumns = array_diff(array_keys($columnDescriptions), $knownColumns);
        if ($missingColumns) {
            $this->descriptionsLogger?->warning(sprintf(
                'Cannot store description of column(s) "%s" of table "%s", the column(s) do not exist.',
                implode('", "', $missingColumns),
                $this->descriptions->getTableId(),
            ));
        }

        return $requestData;
    }
}
