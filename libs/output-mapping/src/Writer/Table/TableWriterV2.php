<?php

namespace Keboola\OutputMapping\Writer\Table;

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
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\Helper\ConfigurationMerger;
use Keboola\OutputMapping\Writer\Helper\DestinationRewriter;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\OutputMapping\Writer\Table;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Temp\Temp;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\SplFileInfo;

class TableWriterV2 extends AbstractWriter
{
    /** @var Metadata */
    private $metadataClient;

    /** @var Table\StrategyInterface */
    private $strategy;

    public function __construct(StrategyFactory $strategyFactory)
    {
        parent::__construct($strategyFactory);
        $this->metadataClient = new Metadata($this->clientWrapper->getBasicClient());
    }

    /**
     * @param string $sourcePathPrefix
     * @param array $configuration
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTableQueue
     * @throws \Exception
     */
    public function uploadTables($sourcePathPrefix, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        if (empty($systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }

        $this->strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput);

        $mappings = $this->strategy->resolveMappings($sourcePathPrefix, $configuration);

        $jobs = [];
        foreach ($mappings as $mapping) {
            $config = $this->resolveTableConfiguration($mapping, $configuration);

            try {
                $jobs[] = $this->uploadTable(
                    $mapping,
                    $config,
                    $systemMetadata
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot upload file "%s" to table "%s" in Storage API: %s',
                        $mapping->getSourceName(),
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
                if (($metadatum['key'] === TableWriter::KBC_LAST_UPDATED_BY_BRANCH_ID) ||
                    ($metadatum['key'] === TableWriter::KBC_CREATED_BY_BRANCH_ID)) {
                    if ((string) $metadatum['value'] === (string) $this->clientWrapper->getBranchId()) {
                        return;
                    }

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
        } catch (ClientException $e) {
            // this is Ok, if the bucket it does not exists, it can't have wrong metadata
            if ($e->getCode() === 404) {
                return;
            }

            throw $e;
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
     * @param Table\Source\SourceInterface $source
     * @param array $config
     * @param array $systemMetadata
     * @return LoadTable
     * @throws ClientException
     */
    private function uploadTable(
        Table\Source\SourceInterface $source,
        array $config,
        array $systemMetadata
    ) {
        if (empty($config['columns']) && is_dir($source->getSourceId())) {
            throw new InvalidOutputException(sprintf('Sliced file "%s" columns specification missing.', $source->getSourceName()));
        }

        if (!$this->isValidTableId($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve valid destination. "%s" is not a valid table ID.',
                $config['destination']
            ));
        }

        $this->ensureValidBucketExists($config['destination'], $systemMetadata);

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
                    $csvFile = new CsvFile($source->getSourceId(), $config['delimiter'], $config['enclosure']);
                    $header = $csvFile->getHeader();
                } catch (Exception $e) {
                    throw new InvalidOutputException('Failed to read file ' . $source->getSourceId() . ' ' . $e->getMessage());
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
                TableWriter::SYSTEM_METADATA_PROVIDER,
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
        if (in_array(TableWriter::TAG_STAGING_FILES_FEATURE, $tokenInfo['owner']['features'], true)) {
            $loadOptions = TagsHelper::addSystemTags($loadOptions, $systemMetadata, $this->logger);
        }

        $tableQueue = $this->strategy->loadDataIntoTable(
            $source,
            $config['destination'],
            $loadOptions
        );

        $tableQueue->addMetadata(new MetadataDefinition(
            $this->clientWrapper->getBasicClient(),
            $config['destination'],
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $this->getUpdatedMetadata($systemMetadata),
            'table'
        ));

        if (!empty($config['metadata'])) {
            $tableQueue->addMetadata(new MetadataDefinition(
                $this->clientWrapper->getBasicClient(),
                $config['destination'],
                $systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID],
                $config['metadata'],
                MetadataDefinition::TABLE_METADATA
            ));
        }

        if (!empty($config['column_metadata'])) {
            $tableQueue->addMetadata(new MetadataDefinition(
                $this->clientWrapper->getBasicClient(),
                $config['destination'],
                $systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID],
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
            TableWriter::SYSTEM_METADATA_PROVIDER,
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
            'key' => TableWriter::KBC_CREATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[TableWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_BRANCH_ID,
                'value' => $systemMetadata[TableWriter::SYSTEM_KEY_BRANCH_ID],
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
     * @param array $systemMetadata
     * @return array
     */
    private function getUpdatedMetadata(array $systemMetadata)
    {
        $metadata[] = [
            'key' => TableWriter::KBC_LAST_UPDATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[TableWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[TableWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_BRANCH_ID,
                'value' => $systemMetadata[TableWriter::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    /**
     * @param Table\Source\SourceInterface $source
     * @param array $configuration
     * @return array
     * @throws InvalidOutputException
     */
    private function resolveTableConfiguration(
        Table\Source\SourceInterface $source,
        array $configuration
    ) {
        $configFromMapping = [];
        $configFromManifest = [];

        if ($source->getMapping() !== null) {
            $configFromMapping = $source->getMapping();
            unset($configFromMapping['source']);
        }

        if ($source->getManifestFile() !== null) {
            $configFromManifest = $this->readTableManifest($source->getManifestFile()->getPathname());

            if (isset($configuration['bucket']) && $this->isTableName($configFromManifest['destination'])) {
                $configFromManifest['destination'] = implode('.', [
                    $configuration['bucket'],
                    basename($source->getSourceName(), '.csv')
                ]);
            }
        }

        if (empty($configFromMapping) && empty($configFromManifest)) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve destination for output table "%s".',
                $source->getSourceName()
            ));
        }

        $config = ConfigurationMerger::mergeConfigurations($configFromManifest, $configFromMapping);

        if (empty($config['destination'])) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve destination for output table "%s".',
                $source->getSourceName()
            ));
        }

        try {
            $config = (new TableManifest())->parse([$config]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidOutputException(
                sprintf("Failed to write manifest for table %s: %s", $source->getSourceName(), $e->getMessage()),
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
                    $source->getSourceName(),
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
    private function ensureValidBucketExists($destination, array $systemMetadata)
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
