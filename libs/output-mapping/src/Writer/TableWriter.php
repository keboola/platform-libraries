<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\OutputMapping\Configuration\Table\Manifest as TableManifest;
use Keboola\OutputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\MetadataDefinition;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class TableWriter extends AbstractWriter
{
    const SYSTEM_METADATA_PROVIDER = 'system';
    const KBC_LAST_UPDATED_BY_BRANCH_ID = 'KBC.lastUpdatedBy.branch.id';
    const KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';
    const KBC_LAST_UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
    const KBC_LAST_UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
    const KBC_CREATED_BY_BRANCH_ID = 'KBC.createdBy.branch.id';
    const KBC_CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
    const KBC_CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
    const KBC_CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';
    const TAG_STAGING_FILES_FEATURE = 'tag-staging-files';

    /** @var Metadata */
    private $metadataClient;

    /** @var Table\StrategyInterface */
    private $strategy;

    /** @var string */
    private $sourcePath;

    public function __construct(StrategyFactory $strategyFactory)
    {
        parent::__construct($strategyFactory);
        $this->metadataClient = new Metadata($this->clientWrapper->getBasicClient());
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
        if (empty($systemMetadata[self::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }
        if ($stagingStorageOutput === StrategyFactory::LOCAL) {
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
        $this->sourcePath = $source;
        $this->strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput);

        /** @var array<string, false|SplFileInfo> $sourcesManifests */
        $sourcesManifests = [];

        // add output mappings fom configuration
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $sourcesManifests[$mapping['source']] = null;
            }
        }
        $processedOutputMappingTables = [];

        $sourcePath = $this->strategy->getMetadataStorage()->getPath() . '/' . $source;

        // add manifest files
        $manifestFiles = ManifestHelper::getManifestFiles($sourcePath);
        foreach ($manifestFiles as $file) {
            $sourcesManifests[$file->getBasename('.manifest')] = $file;
        }

        $jobs = [];
        foreach ($sourcesManifests as $sourceName => $manifestFile) {
            $sourcePath = $sourceName;

            $config = $this->resolveTableConfiguration(
                $configuration,
                $sourceName,
                $manifestFile,
                $processedOutputMappingTables
            );

            try {
                $jobs[] = $this->uploadTable(
                    $config,
                    $sourceName,
                    $sourcePath,
                    $systemMetadata,
                    $stagingStorageOutput
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot upload file "%s" to table "%s" in Storage API: %s',
                        $sourceName,
                        $config["destination"],
                        $e->getMessage()
                    ),
                    $e->getCode(),
                    $e
                );
            }
        }

        $tableQueue = new LoadTableQueue($this->clientWrapper->getBasicClient(), $jobs);
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
        $this->sourcePath = $source;
        $this->strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput);

        if (empty($systemMetadata[self::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }

        $outputMappingTables = [];
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $outputMappingTables[] = $mapping['source'];
            }
        }
        $outputMappingTables = array_unique($outputMappingTables);
        $processedOutputMappingTables = [];

        $sourcePath = $this->strategy->getDataStorage()->getPath() . '/' . $source;

        /** @var array<string, array{SplFileInfo, ?SplFileInfo}> $sources */
        $sources = [];

        $dataFiles = ManifestHelper::getNonManifestFiles($sourcePath);
        foreach ($dataFiles as $file) {
            $sources[$file->getBasename()] = [$file, null];
        }

        $manifestFiles = ManifestHelper::getManifestFiles($sourcePath);
        foreach ($manifestFiles as $file) {
            $dataFileName = $file->getBasename('.manifest');

            if (!isset($sources[$dataFileName])) {
                throw new InvalidOutputException(sprintf('Found orphaned table manifest: "%s"', $file->getBasename()));
            }

            $sources[$dataFileName][1] = $file;
        }

        // Check if all files from output mappings are present
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $filename = $mapping['source'];

                if (!isset($sources[$filename])) {
                    throw new InvalidOutputException(sprintf('Table source "%s" not found.', $mapping['source']), 404);
                }
            }
        }

        $jobs = [];
        foreach ($sources as list($dataFile, $manifestFile)) {
            $sourceName = $dataFile->getBasename();
            $sourcePath = $dataFile->getPathname();

            $config = $this->resolveTableConfiguration(
                $configuration,
                $sourceName,
                $manifestFile,
                $processedOutputMappingTables
            );

            try {
                $jobs[] = $this->uploadTable(
                    $config,
                    $sourceName,
                    $sourcePath,
                    $systemMetadata,
                    $stagingStorageOutput
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot upload file "%s" to table "%s" in Storage API: %s',
                        $sourceName,
                        $config["destination"],
                        $e->getMessage()
                    ),
                    $e->getCode(),
                    $e
                );
            }
        }

        // TODO muze k tomu vubec dojit?
        $processedOutputMappingTables = array_unique($processedOutputMappingTables);
        $diff = array_diff(
            array_merge($outputMappingTables, $processedOutputMappingTables),
            $processedOutputMappingTables
        );
        if (count($diff) > 0) {
            throw new InvalidOutputException(
                sprintf('Can\'t process output mapping for file(s): "%s".', implode('", "', $diff))
            );
        }

        $tableQueue = new LoadTableQueue($this->clientWrapper->getBasicClient(), $jobs);
        $tableQueue->start();
        return $tableQueue;
    }

    /**
     * @param $source
     * @return array
     * @throws InvalidOutputException
     */
    private function readTableManifest($source)
    {
        $adapter = new TableAdapter($this->format);
        $fs = new Filesystem();
        if (!$fs->exists($source)) {
            throw new InvalidOutputException("File '$source' not found.");
        }
        try {
            $fileHandler = new SplFileInfo($source, "", basename($source));
            $serialized = $fileHandler->getContents();
            return $adapter->deserialize($serialized);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                'Failed to read table manifest from file ' . basename($source) . ' ' . $e->getMessage(),
                0,
                $e
            );
        }
    }

    private function checkDevBucketMetadata($destination)
    {
        if (!$this->clientWrapper->hasBranch()) {
            return;
        }
        $bucketId = $this->getBucketId($destination);
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        try {
            foreach ($metadata->listBucketMetadata($bucketId) as $metadatum) {
                if (($metadatum['key'] === self::KBC_LAST_UPDATED_BY_BRANCH_ID) ||
                    ($metadatum['key'] === self::KBC_CREATED_BY_BRANCH_ID)) {
                    if ((string) $metadatum['value'] === (string) $this->clientWrapper->getBranchId()) {
                        return;
                    } else {
                        throw new InvalidOutputException(sprintf(
                            'Trying to create a table in the development bucket "%s" on branch ' .
                            '"%s" (ID "%s"). The bucket metadata marks it as assigned to branch with ID "%s".',
                            $bucketId,
                            $this->clientWrapper->getBranchName(),
                            $this->clientWrapper->getBranchId(),
                            $metadatum['value']
                        ));
                    }
                }
            }
        } catch (ClientException $e) {
            // this is Ok, if the bucket it does not exists, it can't have wrong metadata
            if ($e->getCode() === 404) {
                return;
            } else {
                throw $e;
            }
        }
        throw new InvalidOutputException(sprintf(
            'Trying to create a table in the development ' .
            'bucket "%s" on branch "%s" (ID "%s"), but the bucket is not assigned to any development branch.',
            $bucketId,
            $this->clientWrapper->getBranchName(),
            $this->clientWrapper->getBranchId()
        ));
    }

    /**
     * @param array $config
     * @param string $sourceName
     * @param string $sourcePath
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTable
     * @throws ClientException
     */
    private function uploadTable(
        array $config,
        $sourceName,
        $sourcePath,
        array $systemMetadata,
        $stagingStorageOutput
    ) {
        if (empty($config['columns']) && is_dir($sourcePath)) {
            throw new InvalidOutputException(sprintf('Sliced file "%s" columns specification missing.', $sourceName));
        }

        if (!$this->isValidTableId($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve valid destination. "%s" is not a valid table ID.',
                $config['destination']
            ));
        }

        $this->ensureBucketExists($config['destination'], $systemMetadata);

        // destination table already exists, reuse it
        if ($this->clientWrapper->getBasicClient()->tableExists($config['destination'])) {
            $tableInfo = $this->clientWrapper->getBasicClient()->getTable($config['destination']);
            PrimaryKeyHelper::validatePrimaryKeyAgainstTable($this->logger, $tableInfo, $config);
            if (PrimaryKeyHelper::modifyPrimaryKeyDecider($this->logger, $tableInfo, $config)) {
                PrimaryKeyHelper::modifyPrimaryKey(
                    $this->logger,
                    $this->clientWrapper->getBasicClient(),
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
                $this->clientWrapper->getBasicClient()->deleteTableRows($config['destination'], $deleteOptions);
            }

        // destination table does not exist yet, create it
        } else {
            $primaryKey = implode(",", PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']));
            $distributionKey = implode(",", PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['distribution_key']));

            // columns explicitly configured
            if (!empty($config['columns'])) {
                $this->createTable(
                    $config['destination'],
                    $config['columns'],
                    $primaryKey,
                    $distributionKey ?: null
                );

            // reconstruct columns from CSV header
            } else {
                try {
                    $csvFile = new CsvFile($sourcePath, $config['delimiter'], $config['enclosure']);
                    $header = $csvFile->getHeader();
                } catch (Exception $e) {
                    throw new InvalidOutputException('Failed to read file ' . $sourcePath . ' ' . $e->getMessage());
                }

                $this->createTable(
                    $config['destination'],
                    $header,
                    $primaryKey,
                    $distributionKey ?: null
                );
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
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        if (in_array(self::TAG_STAGING_FILES_FEATURE, $tokenInfo['owner']['features'])) {
            $loadOptions = TagsHelper::addSystemTags($loadOptions, $systemMetadata, $this->logger);
        }

        $tableQueue = $this->loadDataIntoTable($sourcePath, $config['destination'], $loadOptions, $stagingStorageOutput);

        $tableQueue->addMetadata(new MetadataDefinition(
            $this->clientWrapper->getBasicClient(),
            $config['destination'],
            self::SYSTEM_METADATA_PROVIDER,
            $this->getUpdatedMetadata($systemMetadata),
            'table'
        ));

        if (!empty($config['metadata'])) {
            $tableQueue->addMetadata(new MetadataDefinition(
                $this->clientWrapper->getBasicClient(),
                $config['destination'],
                $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
                $config['metadata'],
                MetadataDefinition::TABLE_METADATA
            ));
        }

        if (!empty($config['column_metadata'])) {
            $tableQueue->addMetadata(new MetadataDefinition(
                $this->clientWrapper->getBasicClient(),
                $config['destination'],
                $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
                $config['column_metadata'],
                MetadataDefinition::COLUMN_METADATA
            ));
        }

        return $tableQueue;
    }

    private function getBucketId($tableId)
    {
        $tableIdParts = $this->getTableIdParts($tableId);
        return $tableIdParts[0] . '.' . $tableIdParts[1];
    }

    /**
     * @param string $tableId
     * @return string[]
     */
    private function getTableIdParts($tableId)
    {
        return explode('.', $tableId);
    }

    /**
     * @param string $tableId
     * @return bool
     */
    private function isValidTableId($tableId)
    {
        return count($this->getTableIdParts($tableId)) === 3;
    }

    /**
     * @param string $tableName
     * @return bool
     */
    private function isTableName($tableName)
    {
        return count($this->getTableIdParts($tableName)) === 1;
    }

    private function createBucket($tableId, array $systemMetadata)
    {
        // Create bucket if not exists
        $this->clientWrapper->getBasicClient()->createBucket($this->getBucketName($tableId), $this->getBucketStage($tableId));
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
            'key' => self::KBC_CREATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[self::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => self::KBC_CREATED_BY_BRANCH_ID,
                'value' => $systemMetadata[self::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    private function createTable($tableId, array $columns, $primaryKey, $distributionKey = null)
    {
        $tmp = new Temp();
        $headerCsvFile = new CsvFile($tmp->createFile($this->getTableName($tableId) . '.header.csv'));
        $headerCsvFile->writeRow($columns);
        $options = ['primaryKey' => $primaryKey];
        if (isset($distributionKey)) {
            $options['distributionKey'] = $distributionKey;
        }

        return $this->clientWrapper->getBasicClient()->createTableAsync(
            $this->getBucketId($tableId),
            $this->getTableName($tableId),
            $headerCsvFile,
            $options
        );
    }

    private function getTableName($tableId)
    {
        return $this->getTableIdParts($tableId)[2];
    }

    /**
     * @param string $sourcePath
     * @param string $tableId
     * @param array $options
     * @param string $stagingStorageOutput
     * @return LoadTable
     * @throws ClientException
     */
    private function loadDataIntoTable($sourcePath, $tableId, array $options, $stagingStorageOutput)
    {
        $tags = !empty($options['tags']) ? $options['tags'] : [];
        $this->validateWorkspaceStaging($stagingStorageOutput);
        if ($stagingStorageOutput === StrategyFactory::LOCAL) {
            if (is_dir($sourcePath)) {
                $fileId = $this->uploadSlicedFile($sourcePath, $tags);
                $options['dataFileId'] = $fileId;
                $tableQueue = new LoadTable($this->clientWrapper->getBasicClient(), $tableId, $options);
            } else {
                $fileId = $this->clientWrapper->getBasicClient()->uploadFile(
                    $sourcePath,
                    (new FileUploadOptions())->setCompress(true)->setTags($tags)
                );
                $options['dataFileId'] = $fileId;
                $tableQueue = new LoadTable($this->clientWrapper->getBasicClient(), $tableId, $options);
            }
        } else {
            if ($stagingStorageOutput === StrategyFactory::WORKSPACE_ABS) {
                $sourcePath = $this->specifySourceAbsPath($sourcePath);
            }
            $dataStorage = $this->strategy->getDataStorage();
            $options = [
                'dataWorkspaceId' => $dataStorage->getWorkspaceId(),
                'dataObject' => $sourcePath,
                'incremental' => $options['incremental'],
                'columns' => $options['columns'],
            ];
            $tableQueue = new LoadTable($this->clientWrapper->getBasicClient(), $tableId, $options);
        }
        return $tableQueue;
    }

    private function specifySourceAbsPath($sourcePath)
    {
        $path = $this->ensurePathDelimiter($this->sourcePath) . $sourcePath;
        $absCredentials = $this->strategy->getDataStorage()->getCredentials();
        $blobClient = BlobRestProxy::createBlobService($absCredentials['connectionString']);
        try {
            $options = new ListBlobsOptions();
            $options->setPrefix($path);
            $blobs = $blobClient->listBlobs($absCredentials['container'], $options);
            $isSliced = false;
            foreach ($blobs->getBlobs() as $blob) {
                /* there can be multiple blobs with the same prefix (e.g `my`, `my-file`, ...), we're checking
                    if there are blobs where the prefix is a directory. (e.g `my/` or `my-file/`) */
                if (substr($blob->getName(), 0, strlen($path) + 1) === $path . '/') {
                    $isSliced = true;
                }
            }
            if ($isSliced) {
                $path .= '/';
            }
        } catch (\Exception $e) {
            throw new InvalidOutputException('Failed to list blobs ' . $e->getMessage(), 0, $e);
        }
        return $path;
    }

    protected function ensurePathDelimiter($path)
    {
        return $this->ensureNoPathDelimiter($path) . '/';
    }

    protected function ensureNoPathDelimiter($path)
    {
        return rtrim($path, '\\/');
    }

    /**
     * @param string $stagingStorageOutput
     * @throws InvalidOutputException if not local or valid workspace
     */
    private function validateWorkspaceStaging($stagingStorageOutput)
    {
        $stagingTypes = [
            StrategyFactory::LOCAL,
            StrategyFactory::WORKSPACE_SNOWFLAKE,
            StrategyFactory::WORKSPACE_REDSHIFT,
            StrategyFactory::WORKSPACE_SYNAPSE,
            StrategyFactory::WORKSPACE_ABS,
        ];
        if (!in_array($stagingStorageOutput, $stagingTypes)) {
            throw new InvalidOutputException(
                'Parameter "storage" must be one of: ' .
                implode(', ', $stagingTypes)
            );
        }
    }

    /**
     * Uploads a sliced table to storage api. Takes all files from the $source folder
     *
     * @param string $source Slices folder
     * @return string
     * @throws ClientException
     */
    private function uploadSlicedFile($source, $tags)
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
            ->setCompress(true)
            ->setTags($tags);
        return $this->clientWrapper->getBasicClient()->uploadSlicedFile($sliceFiles, $fileUploadOptions);
    }

    /**
     * @param array $systemMetadata
     * @return array
     */
    private function getUpdatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => self::KBC_LAST_UPDATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[self::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[self::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[self::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => self::KBC_LAST_UPDATED_BY_BRANCH_ID,
                'value' => $systemMetadata[self::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    /**
     * @param array $configuration
     * @param $sourceName
     * @param SplFileInfo $manifestFile
     * @param array $processedOutputMappingTables
     * @return array
     * @throws InvalidOutputException
     */
    private function resolveTableConfiguration(
        array $configuration,
        $sourceName,
        $manifestFile,
        array &$processedOutputMappingTables
    ) {
        $configFromMapping = [];
        $configFromManifest = [];

        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                if (isset($mapping['source']) && $mapping['source'] === $sourceName) {
                    $configFromMapping = $mapping;
                    $processedOutputMappingTables[] = $configFromMapping['source'];
                    unset($configFromMapping['source']);
                }
            }
        }

        if ($manifestFile !== null) {
            $configFromManifest = $this->readTableManifest($manifestFile->getPathname());

            if (isset($configuration['bucket']) && $this->isTableName($configFromManifest['destination'])) {
                $configFromManifest['destination'] = $configuration['bucket'].'.'.basename($sourceName, '.csv');
            }
        }

        $config = ConfigurationMerger::mergeConfigurations($configFromManifest, $configFromMapping);

        if (empty($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve destination for output table "%s".',
                $sourceName
            ));
        }

        try {
            $config = (new TableManifest())->parse([$config]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf("Failed to write manifest for table %s: %s", $sourceName, $e->getMessage()),
                0,
                $e
            );
        }

        $config['primary_key'] = PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']);

        try {
            $config = DestinationRewriter::rewriteDestination($config, $this->clientWrapper);
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Cannot upload file "%s" to table "%s" in Storage API: %s',
                    $sourceName,
                    $config["destination"],
                    $e->getMessage()
                ),
                $e->getCode(),
                $e
            );
        }

        return $config;
    }

    /**
     * @param $destination
     * @param array $systemMetadata
     * @throws ClientException
     */
    private function ensureBucketExists($destination, array $systemMetadata)
    {
        $destinationBucketId = $this->getBucketId($destination);
        $destinationBucketExists = $this->clientWrapper->getBasicClient()->bucketExists($destinationBucketId);

        if (!$destinationBucketExists) {
            $this->createBucket($destination, $systemMetadata);
        } else {
            $this->checkDevBucketMetadata($destination);
        }
    }
}
