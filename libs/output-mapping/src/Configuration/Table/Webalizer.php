<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

use Keboola\OutputMapping\Writer\Helper\RestrictedColumnsHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageNamesSanitizer\ColumnNameSanitizer;
use Psr\Log\LoggerInterface;

class Webalizer
{
    public function __construct(
        readonly private Client $client,
        readonly private LoggerInterface $logger,
        readonly private bool $connectionWebalize,
    ) {
    }

    public function webalize(array $configuration): array
    {
        if (isset($configuration['columns'])) {
            $configuration['columns'] = $this->getWebalizedColumnNames($configuration['columns']);
        }

        if (isset($configuration['primary_key'])) {
            $configuration['primary_key'] = $this->getWebalizedColumnNames($configuration['primary_key']);
        }

        if (isset($configuration['column_metadata'])) {
            $configuration['column_metadata'] = $this->webalizeColumnsMetadata($configuration['column_metadata']);
        }

        if (isset($configuration['schema'])) {
            $configuration['schema'] = $this->webalizeSchemaColumnNames($configuration['schema']);
        }

        return $configuration;
    }

    private function webalizeColumnsMetadata(array $columnsMetadata): array
    {
        $columns = array_keys($columnsMetadata);
        $columns = array_combine($columns, $this->getWebalizedColumnNames($columns));
        foreach ($columnsMetadata as $columnName => $metadata) {
            unset($columnsMetadata[$columnName]);
            $columnsMetadata[$columns[$columnName]] = $metadata;
        }

        return $columnsMetadata;
    }

    private function webalizeSchemaColumnNames(array $schema): array
    {
        $schemaNames = array_map(fn($v) => $v['name'], $schema);

        $schemaNames = array_combine($schemaNames, $this->getWebalizedColumnNames($schemaNames));

        foreach ($schema as $k => $item) {
            $schema[$k]['name'] = $schemaNames[$item['name']];
        }
        return $schema;
    }

    private function getWebalizedColumnNames(array $columns): array
    {
        $columns = array_map('strval', $columns);
        if ($this->connectionWebalize) {
            $webalized = $this->client->webalizeColumnNames($columns);
            $webalized = $webalized['columnNames'];
        } else {
            $webalized = array_map(fn($v) => ColumnNameSanitizer::sanitize($v), $columns);
        }

        foreach ($columns as $k => $column) {
            // System columns should not be webalized and should be preserved
            // they will be validated later in the validator
            if (RestrictedColumnsHelper::isRestrictedColumn($column)) {
                $webalized[$k] = $column;
            } elseif ($webalized[$k] !== $column) {
                $this->logger->warning(sprintf(
                    'Column name "%s" was webalized to "%s"',
                    $column,
                    $webalized[$k],
                ));
            }
        }
        return $webalized;
    }
}
