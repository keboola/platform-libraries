<?php

namespace Keboola\OutputMapping\Writer;

use InvalidArgumentException;
use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Table\TableDefinitionResolver;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\DeferredTasks\TableWriter\AbstractLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Temp\Temp;

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

    /** @var StrategyFactory */
    protected $strategyFactory;

    /** @var Metadata */
    private $metadataClient;

    /** @var TableConfigurationResolver */
    private $tableConfigurationResolver;

    public function __construct(StrategyFactory $strategyFactory)
    {
        parent::__construct($strategyFactory);

        $this->strategyFactory = $strategyFactory;
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
     * @param bool $createTypedTables
     * @return LoadTableQueue
     * @throws \Exception
     */
    public function uploadTables(
        $sourcePathPrefix,
        array $configuration,
        array $systemMetadata,
        $stagingStorageOutput,
        $createTypedTables = false
    ) {
        if (empty($systemMetadata[TableWriter::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }

        $strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput);
        $mappingSources = $strategy->resolveMappingSources($sourcePathPrefix, $configuration);
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
                    $systemMetadata,
                    $createTypedTables
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
        array $systemMetadata,
        bool $createTypedTables
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

        $destinationBucket = $this->ensureDestinationBucket($destination, $systemMetadata);

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
        if ($createTypedTables && !$destinationTableExists && ($hasColumns && !empty($config['column_metadata']))) {
            $tableDefinitionFactory = new TableDefinitionFactory(
                $config['metadata'] ?? [],
                $destinationBucket['backend']
            );
            $tableDefinition = $tableDefinitionFactory->createTableDefinition(
                $destination->getTableName(),
                PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']),
                $config['column_metadata']
            );
            $this->createTableDefinition($destination, $tableDefinition);
            $loadTask = new LoadTableTask($destination, $loadOptions);
            $tableCreated = true;
        } elseif (!$destinationTableExists && $hasColumns) {
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

    /**
     * @param MappingDestination $destination
     * @param array $systemMetadata
     * @return array{id: string, backend: string}
     * @throws ClientException
     */
    private function ensureDestinationBucket(MappingDestination $destination, array $systemMetadata): array
    {
        $destinationBucketId = $destination->getBucketId();
        try {
            $destinationBucketDetails = $this->clientWrapper->getBasicClient()->getBucket($destinationBucketId);
            $this->checkDevBucketMetadata($destination);
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                // bucket doesn't exist so we need to create it
                $this->createDestinationBucket($destination, $systemMetadata);
                $destinationBucketDetails = $this->clientWrapper->getBasicClient()->getBucket($destinationBucketId);
            }
        }

        return [
            'id' => $destinationBucketDetails['id'],
            'backend' => $destinationBucketDetails['backend'],
        ];
    }

    private function createDestinationBucket(MappingDestination $destination, array $systemMetadata): void
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

    private function createTableDefinition(MappingDestination $destination, TableDefinition $tableDefinition)
    {
        $requestData = $tableDefinition->getRequestData();
        $this->clientWrapper->getBasicClient()->createTableDefinition(
            $destination->getBucketId(),
            $requestData
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
