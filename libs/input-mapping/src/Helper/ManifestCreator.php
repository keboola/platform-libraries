<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\StagingProvider\Staging\File\FileFormat;

class ManifestCreator
{
    public function writeTableManifest(
        array $tableInfo,
        string $destination,
        array $columns,
        FileFormat $format,
    ): void {
        $manifest = [
            'id' => $tableInfo['id'],
            'uri' => $tableInfo['uri'],
            'name' => $tableInfo['name'],
            'primary_key' => $tableInfo['primaryKey'],
            'distribution_key' => $tableInfo['distributionKey'],
            'created' => $tableInfo['created'],
            'last_change_date' => $tableInfo['lastChangeDate'],
            'last_import_date' => $tableInfo['lastImportDate'],
        ];
        if (isset($tableInfo['definition']['description']) && $tableInfo['definition']['description'] !== '') {
            $manifest['description'] = $tableInfo['definition']['description'];
        }
        if (isset($tableInfo['s3'])) {
            $manifest['s3'] = $tableInfo['s3'];
        }
        if (isset($tableInfo['abs'])) {
            $manifest['abs'] = $tableInfo['abs'];
        }
        if (!$columns) {
            $columns = $tableInfo['columns'];
        }
        $manifest['columns'] = $columns;

        $manifest['metadata'] = $tableInfo['metadata'];
        foreach ($columns as $column) {
            $columnMetadata = $tableInfo['columnMetadata'][$column] ?? [];
            $manifest['column_metadata'][$column] = $columnMetadata;
        }

        $schema = $this->buildSchema($tableInfo, $columns);
        if ($schema !== null) {
            $manifest['schema'] = $schema;
        }

        $adapter = new TableAdapter($format);
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidInputException $e) {
            throw new InputOperationException(
                sprintf(
                    'Failed to write manifest for table %s - %s.',
                    $tableInfo['id'],
                    $tableInfo['name'],
                ),
                0,
                $e,
            );
        }
    }

    /**
     * Builds the `schema` node describing each selected column's data type, nullability, primary-key flag and
     * description from the table definition. Returns null when the table has no definition.
     *
     * @param string[] $columns
     */
    private function buildSchema(array $tableInfo, array $columns): ?array
    {
        if (empty($tableInfo['definition']['columns'])) {
            return null;
        }

        $backend = $tableInfo['bucket']['backend'] ?? null;
        $primaryKeysNames = $tableInfo['definition']['primaryKeysNames'] ?? [];

        $definitionColumns = [];
        foreach ($tableInfo['definition']['columns'] as $definitionColumn) {
            $definitionColumns[$definitionColumn['name']] = $definitionColumn;
        }

        $schema = [];
        foreach ($columns as $columnName) {
            if (!isset($definitionColumns[$columnName])) {
                continue;
            }
            $definitionColumn = $definitionColumns[$columnName];
            $definition = $definitionColumn['definition'] ?? [];

            $column = ['name' => $columnName];
            $dataType = $this->buildColumnDataType($definitionColumn, $backend);
            if ($dataType !== []) {
                $column['data_type'] = $dataType;
            }
            if (isset($definition['nullable'])) {
                $column['nullable'] = $definition['nullable'];
            }
            $column['primary_key'] = in_array($columnName, $primaryKeysNames, true);
            if (isset($definition['description']) && $definition['description'] !== '') {
                $column['description'] = $definition['description'];
            }
            $schema[] = $column;
        }

        return $schema;
    }

    /**
     * Builds the `data_type` node for a single column: the backend-agnostic `base` type plus the type as it exists
     * on the table's actual backend (length and default are included only when present in the definition). Returns
     * an empty array for non-typed columns, whose definition carries no type information.
     */
    private function buildColumnDataType(array $definitionColumn, ?string $backend): array
    {
        $definition = $definitionColumn['definition'] ?? [];

        $dataType = [];
        if (isset($definitionColumn['basetype'])) {
            $dataType['base'] = ['type' => $definitionColumn['basetype']];
        }
        if ($backend !== null && isset($definition['type'])) {
            $backendType = ['type' => $definition['type']];
            if (isset($definition['length'])) {
                $backendType['length'] = $definition['length'];
            }
            if (isset($definition['default'])) {
                $backendType['default'] = $definition['default'];
            }
            $dataType[$backend] = $backendType;
        }

        return $dataType;
    }

    public function createFileManifest(array $fileInfo): array
    {
        return [
            'id' => $fileInfo['id'],
            'name' => $fileInfo['name'],
            'created' => $fileInfo['created'],
            'is_public' => $fileInfo['isPublic'],
            'is_encrypted' => $fileInfo['isEncrypted'],
            'is_sliced' => $fileInfo['isSliced'],
            'tags' => $fileInfo['tags'],
            'max_age_days' => $fileInfo['maxAgeDays'],
            'size_bytes' => (int) $fileInfo['sizeBytes'],
        ];
    }
}
