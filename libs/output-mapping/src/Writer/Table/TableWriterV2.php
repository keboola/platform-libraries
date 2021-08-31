<?php

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;
use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\LoadTableTask;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
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
        $mappingSources = $strategy->resolveMappingSources($sourcePathPrefix, $configuration);

        foreach ($mappingSources as $source) {
            if ($source->getManifestFile() !== null || $source->getMapping() !== null) {
                continue;
            }

            throw new InvalidOutputException(sprintf(
                'Source table "%s" has neither manifest nor mapping set',
                $source->getSourceName()
            ));
        }

        $defaultBucket = isset($configuration['bucket']) ? $configuration['bucket'] : null;
        $loadTableTasks = [];
        foreach ($mappingSources as $mappingSource) {
            $config = $this->tableConfigurationResolver->resolveTableConfiguration(
                $mappingSource,
                $defaultBucket,
                $systemMetadata
            );

            try {
                $loadTableTasks[] = $this->createLoadTableTask(
                    $strategy,
                    $mappingSource->getSource(),
                    $config,
                    $systemMetadata
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot upload file "%s" to table "%s" in Storage API: %s',
                        $mappingSource->getSourceName(),
                        $config["destination"],
                        $e->getMessage()
                    ),
                    $e->getCode(),
                    $e
                );
            }
        }

        $tableQueue = new LoadTableQueue($this->clientWrapper->getBasicClient(), $loadTableTasks);
        $tableQueue->start();
        return $tableQueue;
    }

    /**
     * @return LoadTableTaskInterface
     * @throws ClientException
     */
    private function createLoadTableTask(
        StrategyInterface $strategy,
        SourceInterface $source,
        array $config,
        array $systemMetadata
    ) {
        $hasColumns = !empty($config['columns']);
        if (!$hasColumns && $source->isSliced()) {
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

        $this->ensureValidDestinationBucketExists($destination, $systemMetadata);

        $storageApiClient = $this->clientWrapper->getBasicClient();
        try {
            $destinationTableInfo = $storageApiClient->getTable($destination->getTableId());
            $destinationTableExists = true;
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }

            $destinationTableInfo = null;
            $destinationTableExists = false;
        }

        if ($destinationTableInfo !== null) {
            PrimaryKeyHelper::validatePrimaryKeyAgainstTable($this->logger, $destinationTableInfo, $config);
            if (PrimaryKeyHelper::modifyPrimaryKeyDecider($this->logger, $destinationTableInfo, $config)) {
                PrimaryKeyHelper::modifyPrimaryKey(
                    $this->logger,
                    $this->clientWrapper->getBasicClient(),
                    $destination->getTableId(),
                    $destinationTableInfo['primaryKey'],
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
                $this->clientWrapper->getBasicClient()->deleteTableRows($destination->getTableId(), $deleteOptions);
            }
        }

        $loadOptions = [
            'columns' => !empty($config['columns']) ? $config['columns'] : [],
            'primaryKey' => implode(',', PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key'])),
            'incremental' => $config['incremental'],
        ];

        if (!$destinationTableExists && isset($config['distribution_key'])) {
            $loadOptions['distributionKey'] = implode(',', PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['distribution_key']));
        }

        $loadOptions = array_merge(
            $loadOptions,
            $strategy->prepareLoadTaskOptions($source, $config)
        );

        // some scenarios are not supported by the SAPI, so we need to take care of them manually here
        // - columns in config + headless CSV (SAPI always expect to have a header in CSV)
        // - sliced files
        if (!$destinationTableExists && $hasColumns) {
            $this->createTable($destination, $config['columns'], $loadOptions);
            $loadTask = new LoadTableTask($destination, $loadOptions);
            $tableCreated = true;
        } elseif ($destinationTableExists) {
            $loadTask = new LoadTableTask($destination, $loadOptions);
            $tableCreated = false;
        } else {
            $loadTask = new CreateAndLoadTableTask($destination, $loadOptions);
            $tableCreated = true;
        }

        if ($tableCreated) {
            $loadTask->addMetadata(new TableMetadata(
                $destination->getTableId(),
                TableWriter::SYSTEM_METADATA_PROVIDER,
                $this->getCreatedMetadata($systemMetadata)
            ));
        }

        $loadTask->addMetadata(new TableMetadata(
            $destination->getTableId(),
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $this->getUpdatedMetadata($systemMetadata)
        ));

        if (!empty($config['metadata'])) {
            $loadTask->addMetadata(new TableMetadata(
                $destination->getTableId(),
                $systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID],
                $config['metadata']
            ));
        }

        if (!empty($config['column_metadata'])) {
            $loadTask->addMetadata(new ColumnMetadata(
                $destination->getTableId(),
                $systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID],
                $config['column_metadata']
            ));
        }

        return $loadTask;
    }

    private function ensureValidDestinationBucketExists(MappingDestination $destination, array $systemMetadata)
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

    private function createTable(MappingDestination $destination, array $columns, array $loadOptions)
    {
        $tmp = new Temp();

        $headerCsvFile = new CsvFile($tmp->createFile($destination->getTableName().'.header.csv'));
        $headerCsvFile->writeRow($columns);

        $this->clientWrapper->getBasicClient()->createTableAsync(
            $destination->getBucketId(),
            $destination->getTableName(),
            $headerCsvFile,
            $loadOptions
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
