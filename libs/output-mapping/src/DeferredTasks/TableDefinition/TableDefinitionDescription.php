<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\TableDefinition;

use Keboola\StorageApi\Client;

/**
 * Applies table and column descriptions through the Storage table-definition update endpoint
 * (PUT /v2/storage/tables/{tableId}/definition). Replaces storing descriptions as KBC.description metadata.
 */
class TableDefinitionDescription
{
    /**
     * @param array<string, string> $columnDescriptions column name => description
     */
    public function __construct(
        private readonly string $tableId,
        private readonly ?string $tableDescription,
        private readonly array $columnDescriptions,
    ) {
    }

    public function hasChanges(): bool
    {
        return $this->tableDescription !== null || $this->columnDescriptions !== [];
    }

    public function apply(Client $client): void
    {
        $payload = [];

        if ($this->tableDescription !== null) {
            $payload['description'] = $this->tableDescription;
        }

        $columns = [];
        foreach ($this->columnDescriptions as $columnName => $description) {
            $columns[] = [
                'name' => $columnName,
                'description' => $description,
            ];
        }
        if ($columns) {
            $payload['columns'] = $columns;
        }

        if (!$payload) {
            return;
        }

        $client->updateTableDefinition($this->tableId, $payload);
    }
}
