<?php

namespace Keboola\InputMapping\Reader;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ManifestWriter
{
    /** @var Client */
    protected $storageClient;

    /** @var string */
    protected $format = 'json';

    public function __construct(Client $storageClient, $format)
    {
        $this->storageClient = $storageClient;
        $this->format = $format;
    }

    /**
     * @param array $tableInfo
     * @param string $destination
     * @param array $columns
     */
    public function writeTableManifest($tableInfo, $destination, $columns)
    {
        $manifest = [
            "id" => $tableInfo["id"],
            "uri" => $tableInfo["uri"],
            "name" => $tableInfo["name"],
            "primary_key" => $tableInfo["primaryKey"],
            "created" => $tableInfo["created"],
            "last_change_date" => $tableInfo["lastChangeDate"],
            "last_import_date" => $tableInfo["lastImportDate"],
        ];
        if (isset($tableInfo["s3"])) {
            $manifest["s3"] = $tableInfo["s3"];
        }
        if (!$columns) {
            $columns = $tableInfo["columns"];
        }
        $manifest["columns"] = $columns;

        $metadata = new Metadata($this->storageClient);
        $manifest['metadata'] = $metadata->listTableMetadata($tableInfo['id']);
        $manifest['column_metadata'] = [];
        foreach ($columns as $column) {
            $manifest['column_metadata'][$column] = $metadata->listColumnMetadata($tableInfo['id'] . '.' . $column);
        }
        $adapter = new TableAdapter($this->format);
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidInputException $e) {
            throw new InputOperationException(
                "Failed to write manifest for table {$tableInfo['id']} - {$tableInfo['name']}.",
                $e
            );
        }
    }

    /**
     * @param $fileInfo
     * @param $destination
     * @throws \Exception
     */
    public function writeFileManifest($fileInfo, $destination)
    {
        $manifest = [
            "id" => $fileInfo["id"],
            "name" => $fileInfo["name"],
            "created" => $fileInfo["created"],
            "is_public" => $fileInfo["isPublic"],
            "is_encrypted" => $fileInfo["isEncrypted"],
            "is_sliced" => $fileInfo["isSliced"],
            "tags" => $fileInfo["tags"],
            "max_age_days" => $fileInfo["maxAgeDays"],
            "size_bytes" => intval($fileInfo["sizeBytes"])
        ];

        $adapter = new FileAdapter($this->format);
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidConfigurationException $e) {
            throw new InputOperationException(
                "Failed to write manifest for file {$fileInfo['id']} - {$fileInfo['name']}.",
                0,
                $e
            );
        }
    }
}
