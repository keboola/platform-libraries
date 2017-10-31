<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader\Reader;
use Keboola\OutputMapping\Configuration\File\Manifest as FileManifest;
use Keboola\OutputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
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
     * @var
     */
    protected $format = 'json';

    /**
     * @var array
     */
    protected $features = [];

    /**
     * @return mixed
     */
    public function getFormat()
    {
        return $this->format;
    }

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
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param Client $client
     * @return $this
     */
    public function setClient(Client $client)
    {
        $this->client = $client;

        return $this;
    }

    /**
     * @param Metadata $metadataClient
     * @return $this
     */
    public function setMetadataClient(Metadata $metadataClient)
    {
        $this->metadataClient = $metadataClient;

        return $this;
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * @param LoggerInterface $logger
     * @return $this
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;

        return $this;
    }

    /**
     * @param array $features
     * @return $this
     */
    public function setFeatures($features)
    {
        $this->features = $features;

        return $this;
    }

    /**
     * @param $feature
     * @return bool
     */
    public function hasFeature($feature)
    {
        return in_array($feature, $this->features);
    }

    /**
     * Writer constructor.
     *
     * @param Client $client
     * @param LoggerInterface $logger
     */
    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->setClient($client);
        $this->setMetadataClient(new Metadata($client));
        $this->setLogger($logger);
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
        $adapter = new FileAdapter($this->getFormat());
        try {
            return $adapter->readFromFile($source);
        } catch (\Exception $e) {
            throw new InvalidOutputException(
                "Failed to parse manifest file $source as " . $this->getFormat() . " " . $e->getMessage(),
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
        $this->getClient()->uploadFile($source, $options);
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
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
                $this->uploadTable($file->getPathname(), $config, $systemMetadata);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$file->getFilename()}' to table '{$config["destination"]}' in Storage API: "
                    . $e->getMessage(),
                    $e
                );
            }

            // After the file has been written, we can write metadata
            if (!empty($config['metadata'])) {
                $this->metadataClient->postTableMetadata(
                    $config["destination"],
                    $systemMetadata['componentId'],
                    $config["metadata"]
                );
            }
            if (!empty($config['column_metadata'])) {
                $this->writeColumnMetadata(
                    $config["destination"],
                    $systemMetadata['componentId'],
                    $config["column_metadata"]
                );
            }
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
        $adapter = new TableAdapter($this->getFormat());
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

    /**
     * @param string $source
     * @param array $config
     * @param array $systemMetadata
     * @throws \Keboola\StorageApi\ClientException
     */
    protected function uploadTable($source, array $config, array $systemMetadata)
    {
        $tableIdParts = explode(".", $config["destination"]);
        $bucketId = $tableIdParts[0] . "." . $tableIdParts[1];
        $bucketName = substr($tableIdParts[1], 2);
        $tableName = $tableIdParts[2];

        if (is_dir($source) && empty($config["columns"])) {
            throw new InvalidOutputException("Sliced file '" . basename($source) . "': columns specification missing.");
        }

        // Create bucket if not exists
        if (!$this->client->bucketExists($bucketId)) {
            $this->client->createBucket($bucketName, $tableIdParts[0]);
            $this->metadataClient->postBucketMetadata(
                $bucketId,
                self::SYSTEM_METADATA_PROVIDER,
                $this->getCreatedMetadata($systemMetadata)
            );
        }

        if ($this->client->tableExists($config["destination"])) {
            $tableInfo = $this->getClient()->getTable($config["destination"]);
            $this->validateAgainstTable($tableInfo, $config);
            if (self::modifyPrimaryKeyDecider($tableInfo, $config, $this->logger)) {
                $this->getLogger()->warning(
                    "Modifying primary key of table {$tableInfo["id"]} from [" .
                    join(", ", $tableInfo["primaryKey"]) . "] to [" . join(", ", $config["primary_key"]) . "]."
                );
                $failed = false;
                // modify primary key
                if (count($tableInfo["primaryKey"]) > 0) {
                    try {
                        $this->client->removeTablePrimaryKey($tableInfo["id"]);
                    } catch (\Exception $e) {
                        // warn and go on
                        $this->getLogger()->warning(
                            "Error deleting primary key of table {$tableInfo["id"]}: " . $e->getMessage()
                        );
                        $failed = true;
                    }
                }
                if (!$failed) {
                    try {
                        if (count($config["primary_key"])) {
                            $this->client->createTablePrimaryKey($tableInfo["id"], $config["primary_key"]);
                        }
                    } catch (\Exception $e) {
                        // warn and try to rollback to original state
                        $this->getLogger()->warning(
                            "Error changing primary key of table {$tableInfo["id"]}: " . $e->getMessage()
                        );
                        if (count($tableInfo["primaryKey"]) > 0) {
                            $this->client->createTablePrimaryKey($tableInfo["id"], $tableInfo["primaryKey"]);
                        }
                    }
                }
            }
            if (!empty($config["delete_where_column"])) {
                // Index columns
                $tableInfo = $this->getClient()->getTable($config["destination"]);
                if (!in_array($config["delete_where_column"], $tableInfo["indexedColumns"])) {
                    $this->getClient()->markTableColumnAsIndexed(
                        $config["destination"],
                        $config["delete_where_column"]
                    );
                }

                // Delete rows
                $deleteOptions = [
                    "whereColumn" => $config["delete_where_column"],
                    "whereOperator" => $config["delete_where_operator"],
                    "whereValues" => $config["delete_where_values"]
                ];
                $this->getClient()->deleteTableRows($config["destination"], $deleteOptions);
            }
            $options = [
                "incremental" => $config["incremental"]
            ];
            // headless csv file
            if (!empty($config["columns"])) {
                $options["columns"] = $config["columns"];
                $options["withoutHeaders"] = true;
            }
            if (is_dir($source)) {
                $options["delimiter"] = $config["delimiter"];
                $options["enclosure"] = $config["enclosure"];
                $this->writeSlicedTable($source, $config["destination"], $options);
            } else {
                $csvFile = new CsvFile($source, $config["delimiter"], $config["enclosure"]);
                $this->client->writeTableAsync($config["destination"], $csvFile, $options);
            }

            $this->metadataClient->postTableMetadata(
                $config['destination'],
                self::SYSTEM_METADATA_PROVIDER,
                $this->getUpdatedMetadata($systemMetadata)
            );
        } else {
            $options = [
                "primaryKey" => join(",", self::normalizePrimaryKey($config["primary_key"], $this->logger))
            ];
            $tableId = $config['destination'];
            // headless csv file
            if (!empty($config["columns"])) {
                $tmp = new Temp();
                $headerCsvFile = new CsvFile($tmp->createFile($tableName . '.header.csv'));
                $headerCsvFile->writeRow($config["columns"]);
                $this->client->createTableAsync($bucketId, $tableName, $headerCsvFile, $options);
                unset($headerCsvFile);
                $options["columns"] = $config["columns"];
                $options["withoutHeaders"] = true;
                if (is_dir($source)) {
                    $options["delimiter"] = $config["delimiter"];
                    $options["enclosure"] = $config["enclosure"];
                    $this->writeSlicedTable($source, $config["destination"], $options);
                } else {
                    $csvFile = new CsvFile($source, $config["delimiter"], $config["enclosure"]);
                    $this->client->writeTableAsync($config["destination"], $csvFile, $options);
                    unset($csvFile);
                }
            } else {
                $csvFile = new CsvFile($source, $config["delimiter"], $config["enclosure"]);
                $tableId = $this->client->createTableAsync($bucketId, $tableName, $csvFile, $options);
                unset($csvFile);
            }
            $this->metadataClient->postTableMetadata(
                $tableId,
                self::SYSTEM_METADATA_PROVIDER,
                $this->getCreatedMetadata($systemMetadata)
            );
        }
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
     */
    protected function writeSlicedTable($source, $destination, $options)
    {
        $finder = new Finder();
        $slices = $finder->files()->in($source)->depth(0);
        $sliceFiles = [];
        /** @var SplFileInfo $slice */
        foreach ($slices as $slice) {
            $sliceFiles[] = $slice->getPathname();
        }
        if (count($sliceFiles) === 0) {
            return;
        }

        // upload slices
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
                ->setIsSliced(true)
                ->setFileName(basename($source))
        ;
        $uploadFileId = $this->client->uploadSlicedFile($sliceFiles, $fileUploadOptions);

        // write table
        $options["dataFileId"] = $uploadFileId;
        $this->client->writeTableAsyncDirect($destination, $options);
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
                        $this->getClient()->addFileTag($file["id"], $tag);
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
