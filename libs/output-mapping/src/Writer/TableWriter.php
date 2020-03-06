<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\MetadataDefinition;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class TableWriter extends AbstractWriter
{
    const SYSTEM_METADATA_PROVIDER = 'system';

    const STAGING_LOCAL = 'local';
    const STAGING_SNOWFLAKE = 'workspace-snowflake';
    const STAGING_REDSHIFT = 'workspace-redshift';

    /**
     * @var Metadata
     */
    private $metadataClient;

    /**
     * @var WorkspaceProviderInterface
     */
    private $workspaceProvider;

    /**
     * AbstractWriter constructor.
     *
     * @param Client $client
     * @param LoggerInterface $logger
     * @param WorkspaceProviderInterface $workspaceProvider
     */
    public function __construct(Client $client, LoggerInterface $logger, WorkspaceProviderInterface $workspaceProvider)
    {
        parent::__construct($client, $logger);
        $this->metadataClient = new Metadata($client);
        $this->workspaceProvider = $workspaceProvider;
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTableQueue
     * @throws \Exception
     */
    public function uploadTables($source, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        if (empty($systemMetadata['componentId'])) {
            throw new OutputOperationException('Component Id must be set');
        }
        if ($stagingStorageOutput === self::STAGING_LOCAL) {
            return $this->uploadTablesLocal($source, $configuration, $systemMetadata, $stagingStorageOutput);
        } else {
            return $this->uploadTablesWorkspace($source, $configuration, $systemMetadata, $stagingStorageOutput);
        }
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTableQueue
     * @throws \Exception
     */
    private function uploadTablesWorkspace($source, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        $finder = new Finder();
        /** @var SplFileInfo[] $files */
        $files = $finder->files()->name('*.manifest')->in($source)->depth(0);

        $sources = [];
        // add output mappings fom configuration
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $sources[$mapping['source']] = false;
            }
        }

        // add manifest files
        foreach ($files as $file) {
            $sources[$file->getBasename('.manifest')] = $file;
        }

        $jobs = [];
        /** @var SplFileInfo|bool $manifest */
        foreach ($sources as $source => $manifest) {
            $configFromMapping = [];
            $configFromManifest = [];
            if (isset($configuration['mapping'])) {
                foreach ($configuration['mapping'] as $mapping) {
                    if (isset($mapping['source']) && $mapping['source'] === $source) {
                        $configFromMapping = $mapping;
                        unset($configFromMapping['source']);
                    }
                }
            }

            $prefix = isset($configuration['bucket']) ? ($configuration['bucket'] . '.') : '';

            if ($manifest !== false) {
                $configFromManifest = $this->readTableManifest($manifest->getPathname());
                if (empty($configFromManifest['destination']) || isset($configuration['bucket'])) {
                    $configFromManifest['destination'] = $this->createDestinationConfigParam(
                        $prefix,
                        $manifest->getBasename('.manifest')
                    );
                }
            } else {
                // If no manifest found and no output mapping, use filename (without .csv if present) as table id
                if (empty($configFromMapping['destination']) || isset($configuration['bucket'])) {
                    $configFromMapping['destination'] = $this->createDestinationConfigParam(
                        $prefix,
                        $manifest->getBasename('.manifest')
                    );
                }
            }

            try {
                $config = array_merge($configFromManifest, $configFromMapping);
                $config = (new TableManifest())->parse([$config]);
                // Mapping with higher priority
                /*
                if ($configFromMapping || !$configFromManifest) {
                    $config = (new TableManifest())->parse([$configFromMapping]);
                } else {
                    $config = (new TableManifest())->parse([$configFromManifest]);
                }
                */
            } catch (InvalidConfigurationException $e) {
                throw new InvalidOutputException(
                    "Failed to write manifest for table {$source}." . $e->getMessage(),
                    0,
                    $e
                );
            }

            try {
                $config['primary_key'] = PrimaryKeyHelper::normalizePrimaryKey($this->logger, $config['primary_key']);
                $tableJob = $this->uploadTable($source, $config, $systemMetadata, $stagingStorageOutput);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$source}' to table '{$config["destination"]}' in Storage API: "
                    . $e->getMessage(),
                    $e->getCode(),
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

        $tableQueue = new LoadTableQueue($this->client, $jobs);
        $tableQueue->start();
        return $tableQueue;
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTableQueue
     * @throws \Exception
     */
    private function uploadTablesLocal($source, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        if (empty($systemMetadata['componentId'])) {
            throw new OutputOperationException('Component Id must be set');
        }
        $manifestNames = ManifestHelper::getManifestFiles($source);

        $finder = new Finder();

        $outputMappingTables = [];
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $outputMappingTables[] = $mapping['source'];
            }
        }
        $outputMappingTables = array_unique($outputMappingTables);
        $processedOutputMappingTables = [];

        /** @var SplFileInfo[] $files */
        $files = $finder->notName('*.manifest')->in($source)->depth(0);

        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[] = $file->getFilename();
        }

        // Check if all files from output mappings are present
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                if (!in_array($mapping['source'], $fileNames)) {
                    throw new InvalidOutputException("Table source '{$mapping["source"]}' not found.", 404);
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
            if (isset($configuration['mapping'])) {
                foreach ($configuration['mapping'] as $mapping) {
                    if (isset($mapping['source']) && $mapping['source'] === $file->getFilename()) {
                        $configFromMapping = $mapping;
                        $processedOutputMappingTables[] = $configFromMapping['source'];
                        unset($configFromMapping['source']);
                    }
                }
            }

            $prefix = isset($configuration['bucket']) ? ($configuration['bucket'] . '.') : '';

            $manifestKey = array_search($file->getPathname() . '.manifest', $manifestNames);
            if ($manifestKey !== false) {
                $configFromManifest = $this->readTableManifest($file->getPathname() . '.manifest');
                if (empty($configFromManifest['destination']) || isset($configuration['bucket'])) {
                    $configFromManifest['destination'] = $this->createDestinationConfigParam(
                        $prefix,
                        $file->getFilename()
                    );
                }
                unset($manifestNames[$manifestKey]);
            } else {
                // If no manifest found and no output mapping, use filename (without .csv if present) as table id
                if (empty($configFromMapping['destination']) || isset($configuration['bucket'])) {
                    $configFromMapping['destination'] = $this->createDestinationConfigParam(
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
                throw new InvalidOutputException(
                    "Failed to write manifest for table {$file->getFilename()}.",
                    0,
                    $e
                );
            }

            if (count(explode('.', $config['destination'])) !== 3) {
                throw new InvalidOutputException(sprintf(
                    'CSV file "%s" file name is not a valid table identifier, either set output mapping for ' .
                        '"%s" or make sure that the file name is a valid Storage table identifier.',
                    $config['destination'],
                    $file->getRelativePathname()
                ));
            }

            try {
                $config['primary_key'] = PrimaryKeyHelper::normalizePrimaryKey($this->logger, $config['primary_key']);
                $tableJob = $this->uploadTable($file->getPathname(), $config, $systemMetadata, $stagingStorageOutput);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$file->getFilename()}' to table '{$config["destination"]}' in Storage API: "
                    . $e->getMessage(),
                    $e->getCode(),
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
                sprintf('Can not process output mapping for file(s): %s.', join('", "', $diff))
            );
        }
        $tableQueue = new LoadTableQueue($this->client, $jobs);
        $tableQueue->start();
        return $tableQueue;
    }

    /**
     * @param $source
     * @return array
     * @throws \Exception
     */
    private function readTableManifest($source)
    {
        $adapter = new TableAdapter($this->format);
        try {
            return $adapter->readFromFile($source);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                'Failed to read table manifest from file ' . basename($source) . ' ' . $e->getMessage(),
                0,
                $e
            );
        }
    }

    /**
     * Creates destination configuration parameter from prefix and file name
     * @param $prefix
     * @param $filename
     * @return string
     */
    private function createDestinationConfigParam($prefix, $filename)
    {
        if (substr($filename, -4) === '.csv') {
            return $prefix . substr($filename, 0, strlen($filename) - 4);
        } else {
            return $prefix . $filename;
        }
    }

    /**
     * @param string $source
     * @param array $config
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTable
     * @throws ClientException
     */
    private function uploadTable($source, array $config, array $systemMetadata, $stagingStorageOutput)
    {
        if (is_dir($source) && empty($config['columns'])) {
            throw new InvalidOutputException(
                sprintf('Sliced file "%s" columns specification missing.', basename($source))
            );
        }
        if (!$this->client->bucketExists($this->getBucketId($config['destination']))) {
            $this->createBucket($config['destination'], $systemMetadata);
        }

        if ($this->client->tableExists($config['destination'])) {
            $tableInfo = $this->client->getTable($config['destination']);
            PrimaryKeyHelper::validatePrimaryKeyAgainstTable($this->logger, $tableInfo, $config);
            if (PrimaryKeyHelper::modifyPrimaryKeyDecider($this->logger, $tableInfo, $config)) {
                PrimaryKeyHelper::modifyPrimaryKey(
                    $this->logger,
                    $this->client,
                    $config['destination'],
                    $tableInfo['primaryKey'],
                    $config['primary_key']
                );
            }
            if (!empty($config['delete_where_column'])) {
                // Delete rows
                $deleteOptions = [
                    'whereColumn' => $config['delete_where_column'],
                    'whereOperator' => $config['delete_where_operator'],
                    'whereValues' => $config['delete_where_values'],
                ];
                $this->client->deleteTableRows($config['destination'], $deleteOptions);
            }
        } else {
            $primaryKey = join(",", PrimaryKeyHelper::normalizePrimaryKey($this->logger, $config['primary_key']));
            if (!empty($config['columns'])) {
                $this->createTable($config['destination'], $config['columns'], $primaryKey);
            } else {
                try {
                    $csvFile = new CsvFile($source, $config['delimiter'], $config['enclosure']);
                    $header = $csvFile->getHeader();
                } catch (Exception $e) {
                    throw new InvalidOutputException('Failed to read file ' . $source . ' ' . $e->getMessage());
                }
                $this->createTable($config['destination'], $header, $primaryKey);
                unset($csvFile);
            }
            $this->metadataClient->postTableMetadata(
                $config['destination'],
                self::SYSTEM_METADATA_PROVIDER,
                $this->getCreatedMetadata($systemMetadata)
            );
        }

        $loadOptions = [
            'delimiter' => $config['delimiter'],
            'enclosure' => $config['enclosure'],
            'columns' => !empty($config['columns']) ? $config['columns'] : [],
            'incremental' => $config['incremental'],
        ];
        $tableQueue = $this->loadDataIntoTable($source, $config['destination'], $loadOptions, $stagingStorageOutput);
        $tableQueue->addMetadata(new MetadataDefinition(
            $this->client,
            $config['destination'],
            self::SYSTEM_METADATA_PROVIDER,
            $this->getUpdatedMetadata($systemMetadata),
            'table'
        ));
        return $tableQueue;
    }

    private function getBucketId($tableId)
    {
        $tableIdParts = $this->getTableIdParts($tableId);
        return $tableIdParts[0] . '.' . $tableIdParts[1];
    }

    private function getTableIdParts($tableId)
    {
        return explode('.', $tableId);
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

    private function getBucketName($tableId)
    {
        return substr($this->getTableIdParts($tableId)[1], 2);
    }

    private function getBucketStage($tableId)
    {
        return $this->getTableIdParts($tableId)[0];
    }

    /**
     * @param array $systemMetadata
     * @return array
     */
    private function getCreatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => 'KBC.createdBy.component.id',
            'value' => $systemMetadata['componentId'],
        ];
        if (!empty($systemMetadata['configurationId'])) {
            $metadata[] = [
                'key' => 'KBC.createdBy.configuration.id',
                'value' => $systemMetadata['configurationId'],
            ];
        }
        if (!empty($systemMetadata['configurationRowId'])) {
            $metadata[] = [
                'key' => 'KBC.createdBy.configurationRow.id',
                'value' => $systemMetadata['configurationRowId'],
            ];
        }
        return $metadata;
    }

    private function createTable($tableId, array $columns, $primaryKey)
    {
        $tmp = new Temp();
        $headerCsvFile = new CsvFile($tmp->createFile($this->getTableName($tableId) . '.header.csv'));
        $headerCsvFile->writeRow($columns);
        $tableId = $this->client->createTableAsync(
            $this->getBucketId($tableId),
            $this->getTableName($tableId),
            $headerCsvFile,
            ['primaryKey' => $primaryKey]
        );
        return $tableId;
    }

    private function getTableName($tableId)
    {
        return $this->getTableIdParts($tableId)[2];
    }

    private function loadDataIntoTable($sourcePath, $tableId, array $options, $stagingStorageOutput)
    {
        if ($stagingStorageOutput === self::STAGING_LOCAL) {
            if (is_dir($sourcePath)) {
                $fileId = $this->uploadSlicedFile($sourcePath);
                $options['dataFileId'] = $fileId;
                $tableQueue = new LoadTable($this->client, $tableId, $options);
            } else {
                $fileId = $this->client->uploadFile(
                    $sourcePath,
                    (new FileUploadOptions())->setCompress(true)
                );
                $options['dataFileId'] = $fileId;
                $tableQueue = new LoadTable($this->client, $tableId, $options);
            }
        } elseif (($stagingStorageOutput === self::STAGING_REDSHIFT) || ($stagingStorageOutput === self::STAGING_SNOWFLAKE)) {
            if ($stagingStorageOutput === self::STAGING_REDSHIFT) {
                $type = 'redshift';
            } else {
                $type = 'snowflake';
            }
            $options = [
                'dataWorkspaceId' => $this->workspaceProvider->getWorkspaceId($type),
                'dataTableName' => $sourcePath,
            ];
            $tableQueue = new LoadTable($this->client, $tableId, $options);
        } else {
            throw new InvalidOutputException(
                'Parameter "storage" must be one of: ' .
                implode(
                    ', ',
                    [self::STAGING_LOCAL, self::STAGING_SNOWFLAKE, self::STAGING_REDSHIFT]
                )
            );
        }
        return $tableQueue;
    }

    /**
     * Uploads a sliced table to storage api. Takes all files from the $source folder
     *
     * @param string $source Slices folder
     * @return string
     * @throws ClientException
     */
    private function uploadSlicedFile($source)
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
     * @param array $systemMetadata
     * @return array
     */
    private function getUpdatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => 'KBC.lastUpdatedBy.component.id',
            'value' => $systemMetadata['componentId'],
        ];
        if (!empty($systemMetadata['configurationId'])) {
            $metadata[] = [
                'key' => 'KBC.lastUpdatedBy.configuration.id',
                'value' => $systemMetadata['configurationId'],
            ];
        }
        if (!empty($systemMetadata['configurationRowId'])) {
            $metadata[] = [
                'key' => 'KBC.lastUpdatedBy.configurationRow.id',
                'value' => $systemMetadata['configurationRowId'],
            ];
        }
        return $metadata;
    }
}
