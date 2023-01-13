<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Configuration\Adapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;

class ManifestCreator
{
    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function writeTableManifest(
        array $tableInfo,
        string $destination,
        array $columns,
        string $format = Adapter::FORMAT_JSON
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

        $adapter = new TableAdapter($format);
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidInputException $e) {
            throw new InputOperationException(
                sprintf(
                    'Failed to write manifest for table %s - %s.',
                    $tableInfo['id'],
                    $tableInfo['name']
                ),
                0,
                $e
            );
        }
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
