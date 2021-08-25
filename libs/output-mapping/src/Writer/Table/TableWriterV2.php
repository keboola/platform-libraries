<?php

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;
use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\MetadataDefinition;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\OutputMapping\Writer\Table;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class TableWriterV2
{
    /** @var StrategyFactory */
    protected $strategyFactory;

    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var Metadata */
    private $metadataClient;

    /** @var TableConfigurationResolver */
    private $tableConfigurationResolver;

    /** @var LoggerInterface */
    private $logger;

    public function __construct(StrategyFactory $strategyFactory)
    {
        $this->strategyFactory = $strategyFactory;
        $this->clientWrapper = $strategyFactory->getClientWrapper();
        $this->metadataClient = new Metadata($this->clientWrapper->getBasicClient());
        $this->tableConfigurationResolver = new TableConfigurationResolver(
            $strategyFactory->getClientWrapper(),
            $strategyFactory->getLogger()
        );
        $this->logger = $strategyFactory->getLogger();
    }

    public function setFormat($format)
    {
        $this->tableConfigurationResolver->setFormat($format);
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

        $strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput);
        $sources = $strategy->resolveMappingSources($sourcePathPrefix, $configuration);

        foreach ($sources as $source) {
            if ($source->getManifestFile() !== null || $source->getMapping() !== null) {
                continue;
            }

            throw new InvalidOutputException(sprintf(
                'Source table "%s" has neither manifest nor mapping set',
                $source->getName()
            ));
        }

        $defaultBucket = isset($configuration['bucket']) ? $configuration['bucket'] : null;
        $jobs = [];
        foreach ($sources as $source) {
            $config = $this->tableConfigurationResolver->resolveTableConfiguration($source, $defaultBucket);

            try {
                $jobs[] = $this->uploadTable(
                    $strategy,
                    $source,
                    $config,
                    $systemMetadata
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot upload file "%s" to table "%s" in Storage API: %s',
                        $source->getName(),
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
     * @return LoadTable
     * @throws ClientException
     */
    private function uploadTable(
        StrategyInterface $strategy,
        Table\MappingSource $source,
        array $config,
        array $systemMetadata
    ) {
        if (empty($config['columns']) && is_dir($source->getId())) {
            throw new InvalidOutputException(sprintf('Sliced file "%s" columns specification missing.', $source->getName()));
        }

        try {
            $destination = new MappingDestination($config['destination']);
        } catch (InvalidArgumentException $e) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve valid destination. "%s" is not a valid table ID.',
                $config['destination']
            ), 0, $e);
        }

        $this->ensureValidBucketExists($destination, $systemMetadata);

        // destination table already exists, reuse it
        $storageApiClient = $this->clientWrapper->getBasicClient();
        if ($storageApiClient->tableExists($config['destination'])) {
            $tableInfo = $storageApiClient->getTable($config['destination']);

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
                    $destination,
                    $config['columns'],
                    $primaryKey,
                    $distributionKey ?: null
                );

            // reconstruct columns from CSV header
            } else {
                try {
                    $csvFile = new CsvFile($source->getId(), $config['delimiter'], $config['enclosure']);
                    $header = $csvFile->getHeader();
                } catch (Exception $e) {
                    throw new InvalidOutputException('Failed to read file ' . $source->getId() . ' ' . $e->getMessage());
                }

                $this->createTable(
                    $destination,
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

        $tableQueue = $strategy->loadDataIntoTable(
            $source->getId(),
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

    /**
     * @param string $primaryKey
     * @param null|string $distributionKey
     */
    private function createTable(MappingDestination $destination, array $columns, $primaryKey, $distributionKey = null)
    {
        $tmp = new Temp();
        $headerCsvFile = new CsvFile($tmp->createFile($destination->getTableName() . '.header.csv'));
        $headerCsvFile->writeRow($columns);
        $options = ['primaryKey' => $primaryKey];
        if (isset($distributionKey)) {
            $options['distributionKey'] = $distributionKey;
        }

        $this->clientWrapper->getBasicClient()->createTableAsync(
            $destination->getBucketId(),
            $destination->getTableName(),
            $headerCsvFile,
            $options
        );
    }

    private function ensureValidBucketExists(MappingDestination $destination, array $systemMetadata)
    {
        $destinationBucketId = $destination->getBucketId();
        $destinationBucketExists = $this->clientWrapper->getBasicClient()->bucketExists($destinationBucketId);

        if (!$destinationBucketExists) {
            $this->createDestinationBucket($destination, $systemMetadata);
        } else {
            $this->checkDevBucketMetadata($destination);
        }
    }

    private function createDestinationBucket(MappingDestination $destination, array $systemMetadata)
    {
        $this->clientWrapper->getBasicClient()->createBucket(
            $destination->getBucketName(),
            $destination->getBucketStage()
        );

        $this->metadataClient->postBucketMetadata(
            $destination->getBucketId(),
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $this->getCreatedMetadata($systemMetadata)
        );
    }

    private function checkDevBucketMetadata(MappingDestination $destination)
    {
        if (!$this->clientWrapper->hasBranch()) {
            return;
        }
        $bucketId = $destination->getBucketId();
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
}
