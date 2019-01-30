<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\InputMapping\Reader\Reader;
use Keboola\OutputMapping\Configuration\File\Manifest as FileManifest;
use Keboola\OutputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\MetadataDefinition;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

/**
 * Class Writer
 * @package Keboola\DockerBundle\Docker\StorageApi
 */
class Writer
{
    const SYSTEM_METADATA_PROVIDER = 'system';

    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Metadata
     */
    protected $metadataClient;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var string
     */
    protected $format = 'json';

    /**
     * @param mixed $format
     * @return $this
     */
    public function setFormat($format)
    {
        $this->format = $format;
        return $this;
    }

    /**
     * Writer constructor.
     *
     * @param Client $client
     * @param LoggerInterface $logger
     */
    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->client = $client;
        $this->metadataClient = new Metadata($client);
        $this->logger = $logger;
    }

    /**
     * Upload files from local temp directory to Storage.
     *
     * @param string $source Source path.
     * @param array $configuration Upload configuration
     */
    public function uploadFiles($source, $configuration = [])
    {

        $manifestNames = $this->getManifestFiles($source);

        $finder = new Finder();
        /** @var SplFileInfo[] $files */
        $files = $finder->files()->notName("*.manifest")->in($source)->depth(0);

        $outputMappingFiles = [];
        if (isset($configuration["mapping"])) {
            foreach ($configuration["mapping"] as $mapping) {
                $outputMappingFiles[] = $mapping["source"];
            }
        }
        $outputMappingFiles = array_unique($outputMappingFiles);
        $processedOutputMappingFiles = [];

        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[] = $file->getFilename();
        }

        // Check if all files from output mappings are present
        if (isset($configuration["mapping"])) {
            foreach ($configuration["mapping"] as $mapping) {
                if (!in_array($mapping["source"], $fileNames)) {
                    throw new InvalidOutputException("File '{$mapping["source"]}' not found.");
                }
            }
        }

        // Check for manifest orphans
        foreach ($manifestNames as $manifest) {
            if (!in_array(substr(basename($manifest), 0, -9), $fileNames)) {
                throw new InvalidOutputException("Found orphaned file manifest: '" . basename($manifest) . "'");
            }
        }

        foreach ($files as $file) {
            $configFromMapping = [];
            $configFromManifest = [];
            if (isset($configuration["mapping"])) {
                foreach ($configuration["mapping"] as $mapping) {
                    if (isset($mapping["source"]) && $mapping["source"] == $file->getFilename()) {
                        $configFromMapping = $mapping;
                        $processedOutputMappingFiles[] = $configFromMapping["source"];
                        unset($configFromMapping["source"]);
                    }
                }
            }
            $manifestKey = array_search($file->getPathname() . ".manifest", $manifestNames);
            if ($manifestKey !== false) {
                $configFromManifest = $this->readFileManifest($file->getPathname() . ".manifest");
                unset($manifestNames[$manifestKey]);
            }
            try {
                // Mapping with higher priority
                if ($configFromMapping || !$configFromManifest) {
                    $storageConfig = (new FileManifest())->parse([$configFromMapping]);
                } else {
                    $storageConfig = (new FileManifest())->parse([$configFromManifest]);
                }
            } catch (InvalidConfigurationException $e) {
                throw new InvalidOutputException("Failed to write manifest for table {$file->getFilename()}.", $e);
            }
            try {
                $this->uploadFile($file->getPathname(), $storageConfig);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$file->getFilename()}' to Storage API: " . $e->getMessage(),
                    $e
                );
            }
        }

        $processedOutputMappingFiles = array_unique($processedOutputMappingFiles);
        $diff = array_diff(
            array_merge($outputMappingFiles, $processedOutputMappingFiles),
            $processedOutputMappingFiles
        );
        if (count($diff)) {
            throw new InvalidOutputException(
                "Couldn't process output mapping for file(s) '" . join("', '", $diff) . "'."
            );
        }
    }

    /**
     * @param $source
     * @return array
     */
    protected function readFileManifest($source)
    {
        $adapter = new FileAdapter($this->format);
        try {
            return $adapter->readFromFile($source);
        } catch (\Exception $e) {
            throw new InvalidOutputException(
                "Failed to parse manifest file $source as " . $this->format . " " . $e->getMessage(),
                $e
            );
        }
    }

    /**
     * @param $source
     * @param array $config
     * @throws \Keboola\StorageApi\ClientException
     */
    protected function uploadFile($source, array $config = [])
    {
        $options = new FileUploadOptions();
        $options
            ->setTags(array_unique($config["tags"]))
            ->setIsPermanent($config["is_permanent"])
            ->setIsEncrypted($config["is_encrypted"])
            ->setIsPublic($config["is_public"])
            ->setNotify($config["notify"]);
        $this->client->uploadFile($source, $options);
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @return LoadTableQueue
     * @throws ClientException
     * @throws \Keboola\Csv\Exception
     */
    public function uploadTables($source, array $configuration, array $systemMetadata)
    {
        if (empty($systemMetadata['componentId'])) {
            throw new OutputOperationException("Component Id must be set");
        }
        $manifestNames = $this->getManifestFiles($source);

        $finder = new Finder();

        /** @var SplFileInfo[] $files */
        $files = $finder->notName("*.manifest")->in($source)->depth(0);

        $outputMappingTables = [];
        if (isset($configuration["mapping"])) {
            foreach ($configuration["mapping"] as $mapping) {
                $outputMappingTables[] = $mapping["source"];
            }
        }
        $outputMappingTables = array_unique($outputMappingTables);
        $processedOutputMappingTables = [];

        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[] = $file->getFilename();
        }

        // Check if all files from output mappings are present
        if (isset($configuration["mapping"])) {
            foreach ($configuration["mapping"] as $mapping) {
                if (!in_array($mapping["source"], $fileNames)) {
                    throw new InvalidOutputException("Table source '{$mapping["source"]}' not found.");
                }
            }
        }

        // Check for manifest orphans
        foreach ($manifestNames as $manifest) {
            if (!in_array(substr(basename($manifest), 0, -9), $fileNames)) {
                throw new InvalidOutputException("Found orphaned table manifest: '" . basename($manifest) . "'");
            }
        }

        $jobs = [];
        foreach ($files as $file) {
            $configFromMapping = [];
            $configFromManifest = [];
            if (isset($configuration["mapping"])) {
                foreach ($configuration["mapping"] as $mapping) {
                    if (isset($mapping["source"]) && $mapping["source"] == $file->getFilename()) {
                        $configFromMapping = $mapping;
                        $processedOutputMappingTables[] = $configFromMapping["source"];
                        unset($configFromMapping["source"]);
                    }
                }
            }

            $prefix = isset($configuration['bucket']) ? ($configuration['bucket'] . '.') : '';

            $manifestKey = array_search($file->getPathname() . ".manifest", $manifestNames);
            if ($manifestKey !== false) {
                $configFromManifest = $this->readTableManifest($file->getPathname() . ".manifest");
                if (empty($configFromManifest["destination"]) || isset($configuration['bucket'])) {
                    $configFromManifest['destination'] = $this->createDestinationConfigParam(
                        $prefix,
                        $file->getFilename()
                    );
                }
                unset($manifestNames[$manifestKey]);
            } else {
                // If no manifest found and no output mapping, use filename (without .csv if present) as table id
                if (empty($configFromMapping["destination"]) || isset($configuration['bucket'])) {
                    $configFromMapping["destination"] = $this->createDestinationConfigParam(
                        $prefix,
                        $file->getFilename()
                    );
                }
            }

            try {
                // Mapping with higher priority
                if ($configFromMapping || !$configFromManifest) {
                    $config = (new TableManifest())->parse([$configFromMapping]);
                } else {
                    $config = (new TableManifest())->parse([$configFromManifest]);
                }
            } catch (InvalidConfigurationException $e) {
                throw new InvalidOutputException("Failed to write manifest for table {$file->getFilename()}.", $e);
            }

            if (count(explode(".", $config["destination"])) != 3) {
                throw new InvalidOutputException(
                    "CSV file '{$config["destination"]}' file name is not a valid table identifier, " .
                    "either set output mapping for '{$file->getRelativePathname()}' or make sure " .
                    "that the file name is a valid Storage table identifier."
                );
            }

            try {
                $config["primary_key"] = self::normalizePrimaryKey($config["primary_key"], $this->logger);
                $tableJob = $this->uploadTable($file->getPathname(), $config, $systemMetadata);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$file->getFilename()}' to table '{$config["destination"]}' in Storage API: "
                    . $e->getMessage(),
                    $e
                );
            }

            // After the file has been written, we can write metadata
            if (!empty($config['metadata'])) {
                $tableJob->addMetadata(
                    new MetadataDefinition(
                        $this->client,
                        $config['destination'],
                        $systemMetadata['componentId'],
                        $config['metadata'],
                        MetadataDefinition::TABLE_METADATA
                    )
                );
            }
            if (!empty($config['column_metadata'])) {
                $tableJob->addMetadata(
                    new MetadataDefinition(
                        $this->client,
                        $config['destination'],
                        $systemMetadata['componentId'],
                        $config['column_metadata'],
                        MetadataDefinition::COLUMN_METADATA
                    )
                );
            }
            $jobs[] = $tableJob;
        }

        $processedOutputMappingTables = array_unique($processedOutputMappingTables);
        $diff = array_diff(
            array_merge($outputMappingTables, $processedOutputMappingTables),
            $processedOutputMappingTables
        );
        if (count($diff)) {
            throw new InvalidOutputException(
                "Couldn't process output mapping for file(s) '" . join("', '", $diff) . "'."
            );
        }
        $tableQueue = new LoadTableQueue($this->client, $jobs);
        $tableQueue->start();
        return $tableQueue;
    }

    /**
     * Creates destination configuration parameter from prefix and file name
     * @param $prefix
     * @param $filename
     * @return string
     */
    protected function createDestinationConfigParam($prefix, $filename)
    {
        if (substr($filename, -4) == '.csv') {
            return $prefix . substr($filename, 0, strlen($filename) - 4);
        } else {
            return $prefix . $filename;
        }
    }

    /**
     * @param $source
     * @return array
     * @throws \Exception
     */
    protected function readTableManifest($source)
    {
        $adapter = new TableAdapter($this->format);
        try {
            return $adapter->readFromFile($source);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                "Failed to read table manifest from file " . basename($source) . ' ' . $e->getMessage(),
                $e
            );
        }
    }

    /**
     * @param array $systemMetadata
     * @return array
     */
    private function getCreatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => 'KBC.createdBy.component.id',
            'value' => $systemMetadata['componentId']
        ];
        if (!empty($systemMetadata['configurationId'])) {
            $metadata[] = [
                'key' => 'KBC.createdBy.configuration.id',
                'value' => $systemMetadata['configurationId']
            ];
        }
        if (!empty($systemMetadata['configurationRowId'])) {
            $metadata[] = [
                'key' => 'KBC.createdBy.configurationRow.id',
                'value' => $systemMetadata['configurationRowId']
            ];
        }
        return $metadata;
    }

    /**
     * @param array $systemMetadata
     * @return array
     */
    private function getUpdatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => 'KBC.lastUpdatedBy.component.id',
            'value' => $systemMetadata['componentId']
        ];
        if (!empty($systemMetadata['configurationId'])) {
            $metadata[] = [
                'key' => 'KBC.lastUpdatedBy.configuration.id',
                'value' => $systemMetadata['configurationId']
            ];
        }
        if (!empty($systemMetadata['configurationRowId'])) {
            $metadata[] = [
                'key' => 'KBC.lastUpdatedBy.configurationRow.id',
                'value' => $systemMetadata['configurationRowId']
            ];
        }
        return $metadata;
    }

    private function getTableIdParts($tableId)
    {
        return explode(".", $tableId);
    }


    private function getBucketName($tableId)
    {
        return substr($this->getTableIdParts($tableId)[1], 2);
    }

    private function getBucketId($tableId)
    {
        $tableIdParts = $this->getTableIdParts($tableId);
        return $tableIdParts[0] . "." . $tableIdParts[1];
    }

    private function getBucketStage($tableId)
    {
        return $this->getTableIdParts($tableId)[0];
    }

    private function getTableName($tableId)
    {
        return $this->getTableIdParts($tableId)[2];
    }

    private function createBucket($tableId, array $systemMetadata)
    {
        // Create bucket if not exists
        $this->client->createBucket($this->getBucketName($tableId), $this->getBucketStage($tableId));
        $this->metadataClient->postBucketMetadata(
            $this->getBucketId($tableId),
            self::SYSTEM_METADATA_PROVIDER,
            $this->getCreatedMetadata($systemMetadata)
        );
    }

    private function createTable($tableId, array $header, array $options)
    {
        $tmp = new Temp();
        $headerCsvFile = new CsvFile($tmp->createFile($this->getTableName($tableId) . '.header.csv'));
        $headerCsvFile->writeRow($header);
        $tableId = $this->client->createTableAsync(
            $this->getBucketId($tableId),
            $this->getTableName($tableId),
            $headerCsvFile,
            $options
        );
        return $tableId;
    }

    private function modifyPrimaryKey($tableId, array $tablePrimaryKey, array $configPrimaryKey)
    {
        $this->logger->warning(
            "Modifying primary key of table {$tableId} from [" .
            join(", ", $tablePrimaryKey) . "] to [" . join(", ", $configPrimaryKey) . "]."
        );
        $failed = false;
        // modify primary key
        if (count($tablePrimaryKey) > 0) {
            try {
                $this->client->removeTablePrimaryKey($tableId);
            } catch (\Exception $e) {
                // warn and go on
                $this->logger->warning(
                    "Error deleting primary key of table {$tableId}: " . $e->getMessage()
                );
                $failed = true;
            }
        }
        if (!$failed) {
            try {
                if (count($configPrimaryKey)) {
                    $this->client->createTablePrimaryKey($tableId, $configPrimaryKey);
                }
            } catch (\Exception $e) {
                // warn and try to rollback to original state
                $this->logger->warning(
                    "Error changing primary key of table {$tableId}: " . $e->getMessage()
                );
                if (count($tablePrimaryKey) > 0) {
                    $this->client->createTablePrimaryKey($tableId, $tablePrimaryKey);
                }
            }
        }
    }

    private function loadData($source, $tableId, array $options)
    {
        if (is_dir($source)) {
            $fileId = $this->uploadSlicedFile($source);
            $options['dataFileId'] = $fileId;
            $tableQueue =  new LoadTable($this->client, $tableId, $options);
        } else {
            $fileId = $this->client->uploadFile(
                $source,
                (new FileUploadOptions())->setCompress(true)
            );
            $options['dataFileId'] = $fileId;
            $tableQueue =  new LoadTable($this->client, $tableId, $options);
        }
        return $tableQueue;
    }

    /**
     * @param string $source
     * @param array $config
     * @param array $systemMetadata
     * @return LoadTable
     * @throws ClientException
     */
    protected function uploadTable($source, array $config, array $systemMetadata)
    {
        $options = [
            "primaryKey" => join(",", self::normalizePrimaryKey($config["primary_key"], $this->logger)),
            "delimiter" => $config["delimiter"],
            "enclosure" => $config["enclosure"],
            "columns" => !empty($config["columns"]) ? $config['columns'] : [],
        ];

        if (is_dir($source) && empty($config["columns"])) {
            throw new InvalidOutputException("Sliced file '" . basename($source) . "': columns specification missing.");
        }
        if (!$this->client->bucketExists($this->getBucketId($config['destination']))) {
            $this->createBucket($config['destination'], $systemMetadata);
        }

        if ($this->client->tableExists($config["destination"])) {
            $tableInfo = $this->client->getTable($config["destination"]);
            $this->validateAgainstTable($tableInfo, $config);
            if (self::modifyPrimaryKeyDecider($tableInfo, $config, $this->logger)) {
                $this->modifyPrimaryKey($config['destination'], $tableInfo['primaryKey'], $config['primary_key']);
            }
            if (!empty($config["delete_where_column"])) {
                // Delete rows
                $deleteOptions = [
                    "whereColumn" => $config["delete_where_column"],
                    "whereOperator" => $config["delete_where_operator"],
                    "whereValues" => $config["delete_where_values"]
                ];
                $this->client->deleteTableRows($config["destination"], $deleteOptions);
            }
        } else {
            if (!empty($config["columns"])) {
                $this->createTable($config['destination'], $config['columns'], $options);
            } else {
                try {
                    $csvFile = new CsvFile($source, $config["delimiter"], $config["enclosure"]);
                    $header = $csvFile->getHeader();
                } catch (Exception $e) {
                    throw new InvalidOutputException('Failed to read file ' . $source . ' ' . $e->getMessage());
                }
                $this->createTable($config['destination'], $header, $options);
                unset($csvFile);
            }
            $this->metadataClient->postTableMetadata(
                $config['destination'],
                self::SYSTEM_METADATA_PROVIDER,
                $this->getCreatedMetadata($systemMetadata)
            );
        }
        $tableQueue = $this->loadData($source, $config['destination'], $options);
        $tableQueue->addMetadata(new MetadataDefinition(
            $this->client,
            $config['destination'],
            self::SYSTEM_METADATA_PROVIDER,
            $this->getUpdatedMetadata($systemMetadata),
            'table'
        ));
        return $tableQueue;
    }

    /**
     * @param $tableId
     * @param $provider
     * @param $columnMetadata
     * @throws ClientException
     */
    protected function writeColumnMetadata($tableId, $provider, $columnMetadata)
    {
        foreach ($columnMetadata as $column => $metadataArray) {
            $columnId = $tableId . "." . $column;
            $this->metadataClient->postColumnMetadata($columnId, $provider, $metadataArray);
        }
    }

    /**
     *
     * Uploads a sliced table to storage api. Takes all files from the $source folder
     *
     * @param string $source Slices folder
     * @param string $destination Destination table
     * @param array $options WriteTable options
     * @param $deferred
     * @return string
     * @throws ClientException
     */
    protected function uploadSlicedFile($source)
    {
        $finder = new Finder();
        $slices = $finder->files()->in($source)->depth(0);
        $sliceFiles = [];
        /** @var SplFileInfo $slice */
        foreach ($slices as $slice) {
            $sliceFiles[] = $slice->getPathname();
        }

        // upload slices
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
                ->setIsSliced(true)
                ->setFileName(basename($source))
                ->setCompress(true);
        return $this->client->uploadSlicedFile($sliceFiles, $fileUploadOptions);
    }

    /**
     * @param string $dir
     * @return array
     */
    protected function getManifestFiles($dir)
    {
        $finder = new Finder();
        $manifests = $finder->files()->name("*.manifest")->in($dir)->depth(0);
        $manifestNames = [];
        /** @var SplFileInfo $manifest */
        foreach ($manifests as $manifest) {
            $manifestNames[] = $manifest->getPathname();
        }
        return $manifestNames;
    }

    /**
     * Add tags to processed input files.
     * @param $configuration array
     */
    public function tagFiles(array $configuration)
    {
        $reader = new Reader($this->client, $this->logger);
        foreach ($configuration as $fileConfiguration) {
            if (!empty($fileConfiguration['processed_tags'])) {
                $files = $reader->getFiles($fileConfiguration);
                foreach ($files as $file) {
                    foreach ($fileConfiguration['processed_tags'] as $tag) {
                        $this->client->addFileTag($file["id"], $tag);
                    }
                }
            }
        }
    }

    /**
     * @param array $tableInfo
     * @param array $config
     */
    public function validateAgainstTable($tableInfo = [], $config = [])
    {
        // primary key
        $configPK = self::normalizePrimaryKey($config["primary_key"], $this->logger);
        if (count($configPK) > 0 || count($tableInfo["primaryKey"]) > 0) {
            if (count(array_diff($tableInfo["primaryKey"], $configPK)) > 0 ||
                count(array_diff($configPK, $tableInfo["primaryKey"])) > 0
            ) {
                $pkMapping = join(", ", $configPK);
                $pkTable = join(", ", $tableInfo["primaryKey"]);
                $message = "Output mapping does not match destination table: primary key '{$pkMapping}' does not match '{$pkTable}' in '{$config["destination"]}'.";
                throw new InvalidOutputException($message);
            }
        }
    }

    /**
     * @param array $pKey
     * @param LoggerInterface $logger
     * @return array
     */
    public static function normalizePrimaryKey(array $pKey, $logger)
    {
        return array_map(
            function ($pKey) {
                return trim($pKey);
            },
            array_unique(
                array_filter($pKey, function ($col) use ($logger) {
                    if ($col != '') {
                        return true;
                    }
                    $logger->warning("Empty primary key found");
                    return false;
                })
            )
        );
    }

    /**
     * @param array $tableInfo
     * @param array $config
     * @param LoggerInterface $logger
     * @return bool
     */
    public static function modifyPrimaryKeyDecider(array $tableInfo, array $config, LoggerInterface $logger)
    {
        $configPK = self::normalizePrimaryKey($config["primary_key"], $logger);
        if (count($tableInfo["primaryKey"]) != count($configPK)) {
            return true;
        }
        if (count(array_intersect($tableInfo["primaryKey"], $configPK)) != count($tableInfo["primaryKey"])) {
            return true;
        }
        return false;
    }
}
